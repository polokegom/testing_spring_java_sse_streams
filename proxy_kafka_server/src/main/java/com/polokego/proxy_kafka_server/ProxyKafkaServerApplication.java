package com.polokego.proxy_kafka_server;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@SpringBootApplication
public class ProxyKafkaServerApplication {

	@RestController
	@CrossOrigin(origins = "http://localhost:4200")
	public class KafkaConsumer{

		//private final SseEmitter sseEmitter;
		private final ExecutorService cachedThreadPool = Executors.newCachedThreadPool();
		//	private final CopyOnWriteArrayList<SseEmitter> emitters = new CopyOnWriteArrayList<>();
		private final CopyOnWriteArrayList<FluxSink<String>> subscribers = new CopyOnWriteArrayList<>();
		private List<String> receivedMessages = new ArrayList<>();

		public KafkaConsumer() {

		}
		@GetMapping("/api/farmhealth")
		public ResponseEntity<List<String>> getMessages() {
			return ResponseEntity.ok(receivedMessages);
		}
/*
		@GetMapping(value = "/stream/farmhealth", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
		public SseEmitter stream() {

			SseEmitter sseEmitter = new SseEmitter();
			emitters.add(sseEmitter);
			sseEmitter.event().reconnectTime(0L);			System.out.println("%%%%%%%%%%%%%%%%%%%%%%%");
			System.out.println(receivedMessages);

			System.out.println("%%%%%%%%%%%%%%%%%%%%%%%%%%");

			sseEmitter.onCompletion(() -> {
				System.out.println("^^^^^^^^^^^^^^^^^^^^^^");

			});


			sseEmitter.onTimeout(() -> {
				System.out.println("$$$$$$$$$$$SSE connection timed out.");
				emitters.remove(sseEmitter);
				sseEmitter.complete();

			});
			return sseEmitter;
		}*/

		//private final CopyOnWriteArrayList<FluxSink<String>> subscribers = new CopyOnWriteArrayList<>();

		@GetMapping(value = "/stream/farmhealth", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
		public Flux<String> stream() {
			return Flux.create(sink -> {
				subscribers.add(sink);

				sink.onDispose(() -> {
					subscribers.remove(sink);
					System.out.println("Subscriber disposed.");
				});
			});
		}


		@KafkaListener(topics = "polokegos-event5", groupId = "group_id7")
		public void consume(String message) {
			System.out.println("Received message: " + message);

			for (FluxSink<String> subscriber : subscribers) {
				subscriber.next(message);
			}
			/*receivedMessages.add(message);
			for (SseEmitter emitter : emitters) {
				try {
					emitter.send(SseEmitter.event().data(message));
				} catch (IOException e) {
					System.out.println("Error sending data to SSE stream: " + e.getMessage());
					// Handle exception or ignore if necessary
				}
			}*/
		}

	}
		public static void main(String[] args) {
			SpringApplication.run(ProxyKafkaServerApplication.class, args);
		}


}
