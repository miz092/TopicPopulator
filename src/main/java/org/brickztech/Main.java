package org.brickztech;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;

public class Main {

    public static void main(String[] args) {
        int numThreads = 8;
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);

        try {
            for (int i = 0; i < numThreads; i++) {
                executor.execute(new ProducerTask(i));
            }
        } finally {
            executor.shutdown();
        }
    }


    private static class ProducerTask implements Runnable {
        private final int partition;

        public ProducerTask(int partition) {
            this.partition = partition;
        }

        public static String generateRandomTimestamp() {
            ThreadLocalRandom random = ThreadLocalRandom.current();

            LocalDate date = LocalDate.now()
                    .minusDays(random.nextInt(2685));

            LocalTime time = LocalTime.of(random.nextInt(0, 23), random.nextInt(0, 59), random.nextInt(0, 59));
            int milliseconds = random.nextInt(0, 999);

            return formatDate(date.atTime(time).plusNanos(milliseconds * 1_000_000));
        }

        public static String formatDate(LocalDateTime date) {
            DateTimeFormatter dtf = DateTimeFormatter.ofPattern("uuuu-MM-dd'T'HH:mm:ss.SSS'Z'");
            return date.atOffset(ZoneOffset.UTC).format(dtf);
        }

        @Override
        public void run() {
            Properties props = new Properties();
            props.put("bootstrap.servers", "ezdf02.hpe4d.local:9092");
            props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            props.put("batch.size", Integer.toString(64 * 1024));
            props.put("linger.ms", "40");
            String json = "{\n" +
                    "    \"syslog-timestamp8601\": \"2023-01-09T12:47:00.184Z\",\n" +
                    "    \"tags\": [\n" +
                    "        \"kubernetes.var.log.pods.nestcld2_bankcalendar-service-7-7nl2s_06e91e26-7301-43c2-a57c-a4506fe2dceb.bankcalendar-service.0.log\",\n" +
                    "        \"validjson\"\n" +
                    "    ],\n" +
                    "    \"confidentiality\": \"3\",\n" +
                    "    \"fingerprint\": \"21506c20b92847f43593f994a4da67b324d922e6b2856bbd5478c61ac9cd4758\",\n" +
                    "    \"message\": \"Received event: [ ModuleInfoRequestEvent[source='java.lang.Object@58873cf5', originService='module-repository:8080:d963981d3cedbf21010a5b98771450f9', destinationService='*:**', id='028058d3-bd1e-4dcd-bca1-324bc0ced22a', timestamp='1673268420003'] ].\",\n" +
                    "    \"timestamp-orig-event\": \"2023-01-09T12:47:00.184703646+00:00\",\n" +
                    "    \"applicationId\": \"APP10216\",\n" +
                    "    \"APP_NAME\": \"bankcalendar-service\",\n" +
                    "    \"serviceCode\": \"auth-server\",\n" +
                    "    \"docker_container_id\": \"2ec28512c9c15f99b969366944ceced6fb66387a6563358a1c0b962395125367\",\n" +
                    "    \"source\": \"n.c.module.client.event.listener.ModuleInfoRequestEventListener:26\",\n" +
                    "    \"time-prelogstash-filter-start\": \"2023-01-09T12:47:07.707Z\",\n" +
                    "    \"openshift\": {\n" +
                    "        \"sequence\": 193221\n" +
                    "    },\n" +
                    "    \"host\": \"oc1wrk23ldv.ocpcendv.ocp.otpbank.hu\",\n" +
                    "    \"io_kubernetes_pod_name\": \"bankcalendar-service-7-7nl2s\",\n" +
                    "    \"io_kubernetes_docker_type\": \"container\",\n" +
                    "    \"logsource\": \"oc1wrk23ldv.ocpcendv.ocp.otpbank.hu\",\n" +
                    "    \"log_type\": \"application\",\n" +
                    "    \"severity\": \"DEBUG\",\n" +
                    "    \"systemId\": \"nestcld\",\n" +
                    "    \"io_kubernetes_pod_uid\": \"06e91e26-7301-43c2-a57c-a4506fe2dceb\",\n" +
                    "    \"io_kubernetes_pod_namespace\": \"nestcld2\",\n" +
                    "    \"docker_container_name\": \"-\",\n" +
                    "    \"io_kubernetes_container_name\": \"bankcalendar-service\",\n" +
                    "    \"@version\": \"1\",\n" +
                    "    \"topic\": \"\",\n" +
                    "    \"@timestamp\": \"2023-01-09T12:47:00.184Z\",\n" +
                    "    \"time-prelogstash-filter-end\": \"2023-01-09T12:47:07.759Z\",\n" +
                    "    \"correlationId\": \"\",\n" +
                    "    \"datetime\": \"2023-01-09T13:47:00.0+0100\",\n" +
                    "    \"syslog-priority\": \"131\"\n" +
                    "}\n";
            try (Producer<String, String> producer = new KafkaProducer<>(props)) {
                List<String> appNames = Arrays.asList(
                        "bankcalendar-service",
                        "fiokert-service",
                        "netbank-service",
                        "fiokert",
                        "netbank"
                );
                String topicName = "general-ocp4-application-logs";
                Instant start = Instant.now();

                for (int i = partition; i < 100_000_000; i += 8) {
                    if (i % 100_000 == 0) {
                        Instant finish = Instant.now();
                        long timeElapsed = Duration.between(start, finish).toMinutes();
                        long timeElapsedSec = Duration.between(start, finish).toSeconds();
                        System.out.println(i + " logs added to general-ocp4-application-logs" + " in " + timeElapsed + " minutes, which is :" + timeElapsedSec + " seconds");
                    }
                    int randomIndex = (int) (Math.random() * appNames.size());

                    String searchKey = "\"APP_NAME\": \"bankcalendar-service\"";
                    String replacement = "\"APP_NAME\": \"" + appNames.get(randomIndex) + "\"";
                    String searchKey2 = "\"@timestamp\": \"2023-01-09T12:47:00.184Z\"";
                    String replacement2 = "\"@timestamp\": \"" + generateRandomTimestamp() + "\"";
                    String newJson = json.replace(searchKey, replacement);
                    String newerJson = newJson.replace(searchKey2, replacement2);
                    ProducerRecord<String, String> record = new ProducerRecord<>(topicName, partition, null, newerJson);

                    producer.send(record, (metadata, exception) -> {
                        if (exception != null) {
                            System.err.println("Error sending message: " + exception.getMessage());
                        }
                    });
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }
}

