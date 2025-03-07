package edu.umich.its.zoomdpsrecordings;

import java.io.File;
import java.util.List;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.spi.IdempotentRepository;
import org.apache.camel.support.processor.idempotent.FileIdempotentRepository;
import org.springframework.stereotype.Component;

/**
 * A simple Camel route that triggers from a timer and calls a bean and prints
 * to system out.
 * <p/>
 * Use <tt>@Component</tt> to make Camel auto detect this route when starting.
 */
@Component
public class PollingRestRouter extends RouteBuilder {
	private volatile String currentToken;

	@Override
	public void configure() throws Exception {
		
		IdempotentRepository repo = FileIdempotentRepository.fileIdempotentRepository(new File(".camel/file.repo"));
		
		/*
		 * from("timer:hello?period={{timer.period}}")
		 * .routeId("hello").transform().method("myBean", "saySomething")
		 * .filter(simple("${body} contains 'foo'")).to("log:foo") .end()
		 * .to("stream:out");
		 */

		// Route to read token at startup
		from("file://{{file.watchdir}}?fileName={{token.file}}&noop=true&idempotent=false&initialDelay=500&repeatCount=1")
				.routeId("readTokenAtStartupRoute")
				.process(new Processor() {
					@Override
					public void process(Exchange exchange) throws Exception {
						currentToken = exchange.getIn().getBody(String.class);
					}
				});

		// File watch route to monitor token changes
		from("file-watch://{{file.watchdir}}?events=MODIFY&antInclude={{token.file}}").routeId("fileWatcherRoute")
				.process(new Processor() {
					@Override
					public void process(Exchange exchange) throws Exception {
						currentToken = exchange.getIn().getBody(String.class);
					}
				});

		// REST call route with dynamic token authentication
		from("timer:tokenPolling?delay=1000&period={{timer.period}}") // 5 minutes in milliseconds
				.routeId("restPollingRoute").process(new Processor() {
					@Override
					public void process(Exchange exchange) throws Exception {
						if (currentToken == null || currentToken.trim().isEmpty()) {
							throw new IllegalStateException("Authentication token not available.");
						}
						exchange.getIn().setHeader("Authorization", "Bearer " + currentToken);
					}
				}).setHeader(Exchange.HTTP_QUERY, simple("{{http_query}}"))
					//.log("Query: $simple{headers}")
				.to("{{rest.service.url}}/phone/recordings/")
				.split().jsonpath("$.recordings[*]", true, List.class)
				  .setHeader("recordingId").jsonpath("id")
				  .setHeader("calleeNumber").jsonpath("callee_number")
				  .setHeader("callerNumber").jsonpath("caller_number")
				  .setHeader("recordingTime").jsonpath("date_time")
				  .setHeader("ownerExtension").jsonpath("owner.extension_number")
				  .setHeader(Exchange.HTTP_URI).jsonpath("download_url", false)
				  .setHeader("CamelHttpMethod", constant("GET"))
				  	.log("Response: ${bodyAs(String)}")
				  .removeHeader(Exchange.HTTP_QUERY)
				  .setBody(constant(""))
				  .choice()
				  	.when(simple("{{filter1}}"))
					  .idempotentConsumer(header("recordingId"), repo)
				  	  .to("{{rest.service.url}}")
				  	  .to("file://{{recording.file.dir}}/{{site1}}?fileName=$simple{header.callerNumber}_$simple{header.recordingTime}.mp3")
				  	  .log("Downloaded {{site1}} recording of ${header.callerNumber}")
				  	  .endChoice()
				  	.when(simple("{{filter2}}"))
				  	  .idempotentConsumer(header("recordingId"), repo)
				  	  .to("{{rest.service.url}}")
				  	  .to("file://{{recording.file.dir}}/{{site2}}?fileName=$simple{header.callerNumber}_$simple{header.recordingTime}.mp3")
				  	  .log("Downloaded {{site2}} recording of ${header.callerNumber}")
				  	  .endChoice();

		  // Route for files for site1
		  from("file://{{recording.file.dir}}/{{site1}}?readLock=changed&delete=true")
		  .setHeader("CamelFileName")
		  .simple("${header.CamelFileName.replaceAll(':','')}") .to(
		  "sftp:{{recording.host}}?disconnect=true&username={{rcs.user}}&password=RAW({{rcs.password}})"
		  );

		  // Route for files for site2
		  from("file://{{recording.file.dir}}/{{site2}}?readLock=changed&delete=true")
		  .setHeader("CamelFileName")
		  .simple("${header.CamelFileName.replaceAll(':','')}") .to(
		  "sftp:{{recording.host}}?disconnect=true&username={{rcs.user}}&password=RAW({{rcs.password}})"
		  );
		}

}