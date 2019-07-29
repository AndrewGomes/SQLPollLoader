package global.game.content.poll;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import global.engine.NamedThreadFactory;
import global.game.world.repository.Repository;
import global.net.mysql.SQLManager;
import global.net.mysql.entry.SQLEntryHandler;
import global.util.logging.SystemLogger;

/**
 * The SQL handler to save and parse the polls.
 * @author Andrew Gomes
 * @date 1/28/2018
 */
public class PollSQLHandler extends SQLEntryHandler<Poll> {

	/**
	 * How often to save the poll results back to the DB in minutes.
	 * The MS handles syncing the online worlds when a player votes by distributing the vote data for that vote.
	 * World 1 will handle sending the info to the SQL to actually save.
	 */
	private static final int BACKUP_PERIOD = 30;

	/**
	 * Constructor
	 */
	public PollSQLHandler() {
		super(null, "", "", "");
	}

	@Override
	public void parse() throws SQLException {
		connection = getConnection();
		if (connection == null) {
			SQLManager.close(connection);
			return;
		}
		int i = 0;
		long millis = System.currentTimeMillis();
		PreparedStatement statement = connection.prepareStatement("SELECT * FROM data_global.poll");
		ResultSet set = statement.executeQuery();
		while (set.next()) {
			i++;
			Poll poll = parsePoll(connection, statement, set);
			if (poll.isActive()) {
				PollRepository.setRunningPoll(poll);
			} else {
				PollRepository.getPreviousPolls().add(poll);
			}
		}
		set.close();
		statement.getConnection().close();
		SystemLogger.log("Parsed " + i + " poll(s) in " + (System.currentTimeMillis() - millis) + "ms...");
		startBackupThread();
	}

	@Override
	public void save() throws SQLException {
		connection = getConnection();
		if (connection == null) {
			SQLManager.close(connection);
			return;
		}
		if (PollRepository.isPollRunning()) {
			Poll poll = PollRepository.getRunningPoll();
			if (poll.getLastVoteCount() >= poll.getVoters().size()) {
				return;
			}
			PreparedStatement statement = null;
			if (!poll.getVoters().isEmpty()) {
				String voters = "";
				for (Integer v : poll.getVoters()) {
					voters += v + ",";
				}
				statement = connection.prepareStatement("UPDATE data_global.poll SET voters='" + (voters.substring(0, voters.length() - 1)) + "' WHERE active=1");
				statement.executeUpdate();
			}
			//Batch update to execute all the questions and all their responses in one go
			statement = connection.prepareStatement("UPDATE data_global.poll_response SET votes=? WHERE response_id=?");
			for (Question q : poll.getQuestions()) {
				for (Response r : q.getResponses()) {
					statement.setInt(1, r.getVotes());
					statement.setInt(2, r.getResponseId());
					statement.addBatch();
				}
			}
			statement.executeBatch();
			statement.getConnection().close();
			poll.setLastVoteCount(poll.getVoters().size());
			SystemLogger.log("Saved the state of the current running poll.");
		}
	}

	@Override
	public void create() throws SQLException {
		;	
	}

	/**
	 * Starts the backup thread and executes it once every X minutes.
	 * The thread only runs on world1 and only if it's not dev mode.
	 */
	public void startBackupThread() {
		SystemLogger.log("Starting poll saving thread - executes every " + (BACKUP_PERIOD) + " mins...");
		ScheduledExecutorService svc = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("PollSave-thread"));
		svc.scheduleAtFixedRate(new Runnable() {
			@Override
			public void run() {
				try {
					save();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}, BACKUP_PERIOD, BACKUP_PERIOD, TimeUnit.MINUTES);
	}

	@Override
	public Connection getConnection() {
		return SQLManager.getConnection();
	}

	/**
	 * Parses a singular poll.
	 * @param connection 
	 * @param statement the statement that was executed.
	 * @param set the result set.
	 * @return the Poll object.
	 * @throws SQLException
	 */
	public static Poll parsePoll(Connection connection, PreparedStatement statement, ResultSet set) throws SQLException {
		int pollId = set.getInt(1);
		boolean active = set.getBoolean(2);
		int year = set.getInt(3);
		String title = set.getString(4);
		String desc = set.getString(5);
		String link = set.getString(6);
		String linkText = set.getString(7);

		//Parse all the people that voted in this particular poll (P-UID)
		ArrayList<Integer> voterList = new ArrayList<Integer>();
		String voters = set.getString(8);
		if (voters != null) {
			for (String uid : voters.split(",")) {
				voterList.add(Integer.parseInt(uid));
			}
		}

		//Parse the questions
		ArrayList<Integer> questionIds = new ArrayList<Integer>();
		ArrayList<Question> questionList = new ArrayList<Question>();
		statement = connection.prepareStatement("SELECT * FROM data_global.poll_question WHERE poll_id=" + pollId);
		ResultSet questionSet = statement.executeQuery();
		while (questionSet.next()) {
			int questionId = questionSet.getInt(1);
			
			String question = questionSet.getString(3);
			String questionLinkText = questionSet.getString(4);
			String questionLink = questionSet.getString(5);
			
			if (questionLink != null && questionLinkText != null)
				questionList.add(new Question(questionId, question, questionLinkText, questionLink));
			else
				questionList.add(new Question(questionId, question));
			
			questionIds.add(questionId);
		}
		questionSet.close();

		//Parse all the responses for the set of questions in this poll in a single statement
		String responseStatement = "SELECT * FROM data_global.poll_response WHERE ";
		for (Integer i : questionIds)
			responseStatement += "question_id=" + i + " OR ";
		
		responseStatement = responseStatement.substring(0, responseStatement.length() - 4);
		statement = connection.prepareStatement(responseStatement);

		//Add the responses to their respective questions
		ResultSet responseSet = statement.executeQuery();
		while (responseSet.next()) {
			int rid = responseSet.getInt(1);
			int qid = responseSet.getInt(2);
			String response = responseSet.getString(3);
			int votes = responseSet.getInt(4);
			for (Question question : questionList) {
				if (question.getId() == qid)
					question.addResponse(new Response(rid, response, votes));
			}
		}

		return new Poll(pollId, active, year, title, desc, link, linkText, questionList, voterList);
	}

	/**
	 * Loads a newly running poll from the database.
	 * This is called when someone uses the -pollupdate command on the MS.
	 * The current poll is saved, moved to history, and cannot be voted on anymore.
	 */
	public static void loadNewRunningPoll() throws SQLException {
		Connection connection = SQLManager.getConnection();
		PreparedStatement statement = connection.prepareStatement("SELECT * FROM data_global.poll WHERE active=1");
		ResultSet set = statement.executeQuery();
		if (set.next()) {
			Poll newRunningPoll = parsePoll(connection, statement, set);
			if (PollRepository.isPollRunning() && (PollRepository.getRunningPoll().getId() == newRunningPoll.getId())) {
				return;
			}
			PollRepository.stopRunningPoll();
			PollRepository.setRunningPoll(newRunningPoll);
			return;
		}
		//No polls are set to active in the db; we're ending the current one.
		if (PollRepository.isPollRunning()) {
			PollRepository.stopRunningPoll();
			Repository.sendNews("The current poll has ended. Thank-you to all those that voted.");
		}
		SQLManager.close(connection);
	}
}