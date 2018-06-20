package sparktest.example;

import java.nio.charset.StandardCharsets;
import java.util.LinkedList;
import java.util.List;

import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.log4j.Logger;
import org.spark_project.jetty.http.HttpStatus;

public class RestLogger extends Logger {

	public RestLogger(Logger target) {
		super(target.getName());
		shadowedLogger = target;
	}

	private Logger shadowedLogger;

	private HttpClient httpClient = HttpClients.createDefault();

	private void log(Object msg) {
		HttpPost postRequest = new HttpPost("http://log-catcher:8080/logCatcher/log");
		List<NameValuePair> params = new LinkedList<>();

		params.add(new BasicNameValuePair("identifier", System.getenv("HOSTNAME")));
		params.add(new BasicNameValuePair("message", msg.toString()));

		postRequest.setEntity(new UrlEncodedFormEntity(params, StandardCharsets.UTF_8));
		HttpResponse response;
		try {
			response = httpClient.execute(postRequest);
		} catch (Exception e) {
			shadowedLogger.error("Failed to send message to log-catcher instance.", e);
			return;
		}
		int status = response.getStatusLine().getStatusCode();
		if (status >= HttpStatus.OK_200 && status < HttpStatus.MULTIPLE_CHOICES_300) {
			return; // yay!
		}
		if (status < HttpStatus.OK_200 || status >= HttpStatus.INTERNAL_SERVER_ERROR_500) {
			shadowedLogger.error("There was an internal server error while reporting (Status code " + status + ")");
		}
		shadowedLogger.error("There was a user error while reporting (Status code " + status + ")");

	}

	private void log(Object msg, Throwable e) {
		log(msg + "\n" + e.getLocalizedMessage());
	}

	@Override
	public void debug(Object message) {
		shadowedLogger.debug(message);
		log("[debug] " + this.getName() + ": " + message);
	}

	@Override
	public void debug(Object message, Throwable e) {
		shadowedLogger.debug(message, e);
		log("[debug] " + this.getName() + ": " + message, e);
	}

	@Override
	public void error(Object message) {
		shadowedLogger.error(message);
		log("[error] " + this.getName() + ": " + message);
	}

	@Override
	public void error(Object message, Throwable e) {
		shadowedLogger.error(message, e);
		log("[error] " + this.getName() + ": " + message, e);
	}

	@Override
	public void fatal(Object message) {
		shadowedLogger.fatal(message);
		log("[fatal] " + this.getName() + ": " + message);
	}

	@Override
	public void fatal(Object message, Throwable e) {
		shadowedLogger.fatal(message, e);
		log("[fatal] " + this.getName() + ": " + message, e);
	}

	@Override
	public void info(Object message) {
		shadowedLogger.info(message);
		log("[info] " + this.getName() + ": " + message);
	}

	@Override
	public void info(Object message, Throwable e) {
		shadowedLogger.info(message, e);
		log("[info] " + this.getName() + ": " + message, e);
	}

	@Override
	public void warn(Object message) {
		shadowedLogger.warn(message);
		log("[warn] " + this.getName() + ": " + message);
	}

	@Override
	public void warn(Object message, Throwable e) {
		shadowedLogger.warn(message, e);
		log("[warn] " + this.getName() + ": " + message, e);
	}

	/**
	 * Log a message object with the {@link org.apache.log4j.Level#TRACE TRACE}
	 * level.
	 *
	 * @param message
	 *            the message object to log.
	 * @see #debug(Object) for an explanation of the logic applied.
	 * @since 1.2.12
	 */
	@Override
	public void trace(Object message) {
		shadowedLogger.trace(message);
		log("[TRACE] " + this.getName() + ": " + message);
	}

	/**
	 * Log a message object with the <code>TRACE</code> level including the stack
	 * trace of the {@link Throwable}<code>t</code> passed as parameter.
	 *
	 * <p>
	 * See {@link #debug(Object)} form for more detailed information.
	 * </p>
	 *
	 * @param message
	 *            the message object to log.
	 * @param t
	 *            the exception to log, including its stack trace.
	 * @since 1.2.12
	 */
	@Override
	public void trace(Object message, Throwable t) {
		shadowedLogger.trace(message, t);
		log(message, t);
	}

}
