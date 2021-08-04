package org.hps;

import io.fabric8.kubernetes.api.model.ServiceAccount;
import io.fabric8.kubernetes.api.model.ServiceAccountBuilder;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.ExecutionException;




//this is a working version
public class Scaler {
    static RLS rls;
    static double[][] regArr;
    static RealMatrix regMatrix;
    static int iteration;
    static int num_vars;

    Scaler() {
    }

    public static  String CONSUMER_GROUP;
    public static int numberOfPartitions;
    static boolean scaled = false;
    public static AdminClient admin = null;
    private static final Logger log = LogManager.getLogger(Scaler.class);
    public static Map<TopicPartition, Long> currentPartitionToCommittedOffset = new HashMap<>();
    public static Map<TopicPartition, Long> previousPartitionToCommittedOffset = new HashMap<>();
    public static Map<TopicPartition, Long> previousPartitionToLastOffset = new HashMap<>();
    public static Map<TopicPartition, Long> currentPartitionToLastOffset = new HashMap<>();
    public static Map<TopicPartition, Long> partitionToLag = new HashMap<>();
    public static Map<MemberDescription, Float> maxConsumptionRatePerConsumer = new HashMap<>();
    public static Map<MemberDescription, Long> consumerToLag = new HashMap<>();

    public static  String mode;



    static boolean firstIteration = true;
    static Long sleep;
    static Long waitingTime;
    static String topic;
    static String cluster;
    static Long poll;
    static Long SEC;
    static String choice;
    static Float uth;
    static Float dth;
    static double prediction = 0;
    private static Instant start = null;
    static long elapsedTime;
    static String BOOTSTRAP_SERVERS;

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        //TODO Externalize topic, cluster name, and all configurations
        //TODO externalize autoscale decision logic Arriva or lag
        sleep = Long.valueOf(System.getenv("SLEEP"));
        waitingTime = Long.valueOf(System.getenv("WAITING_TIME"));
        topic = System.getenv("TOPIC");
        cluster = System.getenv("CLUSTER");
        poll = Long.valueOf(System.getenv("POLL"));
        SEC = Long.valueOf(System.getenv("SEC"));
        choice = System.getenv("CHOICE");
        uth= Float.parseFloat(System.getenv("uth"));
        dth= Float.parseFloat(System.getenv("dth"));
        CONSUMER_GROUP = System.getenv("CONSUMER_GROUP");
        mode = System.getenv("Mode");
        BOOTSTRAP_SERVERS = System.getenv("BOOTSTRAP_SERVERS");


        log.info("sleep is {}", sleep);
        log.info("waiting time  is {}", waitingTime);
        log.info("topic is  {}", topic);
        log.info("poll is  {}", poll);
        log.info("SEC is  {}", SEC);
        log.info("uth is  {}", uth);
        log.info("dth is {}", dth);

        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 3000);
        props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 3000);
        admin = AdminClient.create(props);
        /////////////////////////////////////////////////////////////
        num_vars = 4;
        rls = new RLS(num_vars, 0.98);
        iteration = 0;
        regArr = new double[1][num_vars];

        for (int j = 0; j < num_vars; j++)
            regArr[0][j] = 0;


        //regressor array
        regMatrix = new Array2DRowRealMatrix(regArr);

        ///////////////////////////////////////////////////////////////////////////////////////
        while (true) {
            log.info("=================Start Iteration===============" +
                    "=======");
            log.info("Iteration {}", iteration);

            //get committed  offsets
            Map<TopicPartition, OffsetAndMetadata> offsets =
                    admin.listConsumerGroupOffsets(CONSUMER_GROUP)
                            .partitionsToOffsetAndMetadata().get();
            numberOfPartitions = offsets.size();
            Map<TopicPartition, OffsetSpec> requestLatestOffsets = new HashMap<>();
            //initialize consumer to lag to 0
            for (TopicPartition tp : offsets.keySet()) {
                requestLatestOffsets.put(tp, OffsetSpec.latest());
                partitionToLag.put(tp, 0L);
            }
            //blocking call to query latest offset
            Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> latestOffsets =
                    admin.listOffsets(requestLatestOffsets).all().get();
            //////////////////////////////////////////////////////////////////////
            // Partition Statistics
            /////////////////////////////////////////////////////////////////////
            for (Map.Entry<TopicPartition, OffsetAndMetadata> e : offsets.entrySet()) {
                long committedOffset = e.getValue().offset();
                long latestOffset = latestOffsets.get(e.getKey()).offset();
                long lag = latestOffset - committedOffset;

                if (!firstIteration) {
                    previousPartitionToCommittedOffset.put(e.getKey(), currentPartitionToCommittedOffset.get(e.getKey()));
                    previousPartitionToLastOffset.put(e.getKey(), currentPartitionToLastOffset.get(e.getKey()));
                }
                currentPartitionToCommittedOffset.put(e.getKey(), committedOffset);
                currentPartitionToLastOffset.put(e.getKey(), latestOffset);
                partitionToLag.put(e.getKey(), lag);
            }
            //////////////////////////////////////////////////////////////////////
            // consumer group statistics
            /////////////////////////////////////////////////////////////////////
            /* lag per consumer */
            Long lag = 0L;
            //get information on consumer groups, their partitions and their members
            DescribeConsumerGroupsResult describeConsumerGroupsResult =
                    admin.describeConsumerGroups(Collections.singletonList(Scaler.CONSUMER_GROUP));
            KafkaFuture<Map<String, ConsumerGroupDescription>> futureOfDescribeConsumerGroupsResult =
                    describeConsumerGroupsResult.all();
            Map<String, ConsumerGroupDescription> consumerGroupDescriptionMap = futureOfDescribeConsumerGroupsResult.get();

            // if a particular consumer is removed as a result of scaling decision remove
            Set<MemberDescription> previousConsumers = new HashSet<MemberDescription>(consumerToLag.keySet());
            for (MemberDescription md : previousConsumers) {
                if (consumerGroupDescriptionMap.get(Scaler.CONSUMER_GROUP).members().contains(md)) {
                    continue;
                }
                consumerToLag.remove(md);
                maxConsumptionRatePerConsumer.remove(md);
            }
            //////////////////////////////////
            for (MemberDescription memberDescription : consumerGroupDescriptionMap.get(Scaler.CONSUMER_GROUP).members()) {
                MemberAssignment memberAssignment = memberDescription.assignment();
                for (TopicPartition tp : memberAssignment.topicPartitions()) {
                    lag += partitionToLag.get(tp);
                }
                consumerToLag.put(memberDescription, lag);
                lag = 0L;
            }
            if(!firstIteration) {
                //justPredict3(consumerGroupDescriptionMap);
                if(mode.equalsIgnoreCase("proactive")){
                    justPredict3(consumerGroupDescriptionMap);
                } else {
                    scaleDecision2(consumerGroupDescriptionMap);
                }

            } else {
                firstIteration = false;
            }

            log.info("sleeping for  {} secs", sleep);
            log.info("====================End Iteration=====" +
                    "==========");
            Thread.sleep(sleep);
        }
    }


    static void justPredict2(Map<String, ConsumerGroupDescription> consumerGroupDescriptionMap) {
        float totalConsumptionRate = 0;
        float totalArrivalRate = 0;
        long totallag = 0;
        int size = consumerGroupDescriptionMap.get(Scaler.CONSUMER_GROUP).members().size();
        log.info("Currently we have this number of consumers {}", size);
        long totalpoff = 0;
        long totalcoff = 0;
        long totalepoff = 0;
        long totalecoff = 0;

        for (MemberDescription memberDescription : consumerGroupDescriptionMap.get(Scaler.CONSUMER_GROUP).members()) {
            totalpoff = 0;
            totalcoff = 0;
            totalepoff = 0;
            totalecoff = 0;
            for (TopicPartition tp : memberDescription.assignment().topicPartitions()) {
                totalpoff += previousPartitionToCommittedOffset.get(tp);
                totalcoff += currentPartitionToCommittedOffset.get(tp);
                totalepoff += previousPartitionToLastOffset.get(tp);
                totalecoff += currentPartitionToLastOffset.get(tp);
            }
            float consumptionRatePerConsumer = (float) (totalcoff - totalpoff) / sleep;
            float arrivalRatePerConsumer = (float) (totalecoff - totalepoff) / sleep;

            if (arrivalRatePerConsumer >= consumptionRatePerConsumer) {
                // TODO do something
            }
            if (consumptionRatePerConsumer >
                    maxConsumptionRatePerConsumer.getOrDefault(memberDescription, 0.0f)) {
                maxConsumptionRatePerConsumer.put(memberDescription, consumptionRatePerConsumer);
            }
            totalConsumptionRate += consumptionRatePerConsumer;
            totalArrivalRate += arrivalRatePerConsumer;
            totallag += consumerToLag.get(memberDescription);
        }
        log.info("=================================:");
        log.info("totalArrivalRate {}, totalconsumptionRate {}, totallag {}",
                totalArrivalRate * 1000, totalConsumptionRate * 1000, totallag);
        ///////////////////////////////////////////////////prediction code//////////////////////////////////////////
        if (iteration < 3) {
            double y1 = Double.parseDouble(String.valueOf((totalArrivalRate*1000)));
            log.info(" iteration {} last time  prediction and arrival rate {}", iteration, (float) (prediction)) ;
            log.info(" iteration {} current actual value of  arrival rate = {}", iteration, y1);
            rls.add_obs(regMatrix.transpose(), y1);
            for(int j = 0; j < num_vars-1; j++)
                regArr[0][j] = regArr[0][j + 1];
            regArr[0][num_vars-1] = y1;
            regMatrix = new Array2DRowRealMatrix(regArr);
            log.info(regMatrix);
            prediction = (rls.getW().transpose().multiply(regMatrix.transpose())).getEntry(0, 0);
            log.info(" Iteration {} prediction  next arrival rate {}", iteration, (float) (prediction));
        } else {
            double y1 = Double.parseDouble(String.valueOf((totalArrivalRate*1000)));
            log.info(" iteration {} last time  prediction  and arrival rate {}", iteration, (float) (prediction)) ;
            log.info(" iteration {} current actual value of  arrival rate = {}", iteration, y1);
            rls.add_obs(regMatrix.transpose(), y1);
            for(int j = 0; j < num_vars-1; j++)
                regArr[0][j] = regArr[0][j + 1];
            regArr[0][num_vars-1] = y1;
            regMatrix = new Array2DRowRealMatrix(regArr);
            log.info(regMatrix);
            prediction = (rls.getW().transpose().multiply(regMatrix.transpose())).getEntry(0, 0);
            log.info(" Iteration {} prediction for next  arrival rate is {}", iteration, (float) (prediction));
            log.info("=================================:");
        }
        iteration++;
    }


    static void justPredict3(Map<String, ConsumerGroupDescription> consumerGroupDescriptionMap) {
        float totalConsumptionRate = 0;
        float totalArrivalRate = 0;
        long totallag = 0;
        int size = consumerGroupDescriptionMap.get(Scaler.CONSUMER_GROUP).members().size();
        log.info("Currently we have this number of consumers {}", size);

        long totalpoff;
        long totalcoff;
        long totalepoff;
        long totalecoff;
        for (MemberDescription memberDescription : consumerGroupDescriptionMap.get(Scaler.CONSUMER_GROUP).members()) {
            totalpoff = 0;
            totalcoff = 0;
            totalepoff = 0;
            totalecoff = 0;
            for (TopicPartition tp : memberDescription.assignment().topicPartitions()) {
                totalpoff += previousPartitionToCommittedOffset.get(tp);
                totalcoff += currentPartitionToCommittedOffset.get(tp);
                totalepoff += previousPartitionToLastOffset.get(tp);
                totalecoff += currentPartitionToLastOffset.get(tp);
            }
            float consumptionRatePerConsumer = (float) (totalcoff - totalpoff) / sleep;
            float arrivalRatePerConsumer = (float) (totalecoff - totalepoff) / sleep;
            if (arrivalRatePerConsumer >= consumptionRatePerConsumer) {
                // TODO do something
            }
            if (consumptionRatePerConsumer >
                    maxConsumptionRatePerConsumer.getOrDefault(memberDescription, 0.0f)) {
                maxConsumptionRatePerConsumer.put(memberDescription, consumptionRatePerConsumer);
            }
            totalConsumptionRate += consumptionRatePerConsumer;
            totalArrivalRate += arrivalRatePerConsumer;
            totallag += consumerToLag.get(memberDescription);

        }
        log.info("=================================:");
        log.info("totalArrivalRate {}, totalconsumptionRate {}, totallag {}",
                totalArrivalRate * 1000, totalConsumptionRate * 1000, totallag);
        ///////////////////////////////////////////////////prediction code//////////////////////////////////////////
        if (iteration <= 10) {////the model is in training phase  reactive autoscale
            double y1 = Double.parseDouble(String.valueOf((totalArrivalRate * 1000)));
            log.info(" iteration {} last time  prediction and arrival rate {}", iteration, (float) (prediction));
            log.info(" iteration {} current actual value of  arrival rate = {}", iteration, y1);
            rls.add_obs(regMatrix.transpose(), y1);
            for(int j = 0; j <  num_vars -1; j++)
                regArr[0][j] = regArr[0][j + 1];
            regArr[0][num_vars -1] = y1;
            regMatrix = new Array2DRowRealMatrix(regArr);
            log.info(regMatrix);
            prediction = (rls.getW().transpose().multiply(regMatrix.transpose())).getEntry(0, 0);
            log.info(" Iteration {} prediction  next arrival rate {}", iteration, (float) (prediction));
          //  iteration++;
            if ((totalArrivalRate * 1000) > (size * poll)) {
                if (size < numberOfPartitions) {
                    log.info("Consumers are less than nb partition we can scale");
                    try (final KubernetesClient k8s = new DefaultKubernetesClient()) {
                        ServiceAccount fabric8 = new ServiceAccountBuilder().withNewMetadata().withName("fabric8").endMetadata().build();
                        k8s.serviceAccounts().inNamespace("default").createOrReplace(fabric8);
                        k8s.apps().deployments().inNamespace("default").withName("cons1persec").scale(size + 1);
                        //scaled = true;
                        start = Instant.now();
                        log.info("since  arrival rate  predicted  {} is greater than  maximum consumed messages rate (size*poll) ,  I up scaled  by one {}",
                                totalArrivalRate * 1000, (size * poll));
                    }
                } else {
                    log.info("Consumers are equal to nb partitions we can not scale up anymore");
                }
            } else if ((totalArrivalRate * 1000) < ((size - 1) * poll)) {
                try (final KubernetesClient k8s = new DefaultKubernetesClient()) {
                    ServiceAccount fabric8 = new ServiceAccountBuilder().withNewMetadata().withName("fabric8").endMetadata().build();
                    k8s.serviceAccounts().inNamespace("default").createOrReplace(fabric8);
                    int replicas = k8s.apps().deployments().inNamespace("default").withName("cons1persec").get().getSpec().getReplicas();
                    if (replicas > 1) {
                        k8s.apps().deployments().inNamespace("default").withName("cons1persec").scale(replicas - 1);
                        scaled = true;
                        //firstIteration = true;
                        start = Instant.now();
                        log.info("since   arrival rate {} is lower than max   consumption rate  with size -1 times dth, I down scaled  by one {}",
                                totalArrivalRate * 1000,
                                ((size - 1) * poll * dth) / (float) SEC);
                    } else {
                        log.info("Not going to  down scale since replicas already one");
                    }
                }
            }
        } else {//the model is trained proactive autoscale
            double y1 = Double.parseDouble(String.valueOf((totalArrivalRate * 1000)));
            log.info(" iteration {} last time  prediction  of arrival rate {}", iteration, (float) (prediction));
            log.info(" iteration {} current actual value of  arrival rate = {}", iteration, y1);
            rls.add_obs(regMatrix.transpose(), y1);
            for (int j = 0; j < num_vars - 1; j++)
                regArr[0][j] = regArr[0][j + 1];

            regArr[0][num_vars-1] = y1;

            regMatrix = new Array2DRowRealMatrix(regArr);
            log.info(regMatrix);
            prediction = (rls.getW().transpose().multiply(regMatrix.transpose())).getEntry(0, 0);
            log.info(" Iteration {} prediction for next  arrival rate is {}", iteration, (float) (prediction));

            if ((prediction) > (size * poll) /*|| (totalArrivalRate*1000) > (size *poll)*/) /*totalConsumptionRate *1000*/ {
                if (size < numberOfPartitions) {
                    log.info("Consumers are less than nb partition we can scale");
                    try (final KubernetesClient k8s = new DefaultKubernetesClient()) {
                        ServiceAccount fabric8 = new ServiceAccountBuilder().withNewMetadata().withName("fabric8").endMetadata().build();
                        k8s.serviceAccounts().inNamespace("default").createOrReplace(fabric8);
                        k8s.apps().deployments().inNamespace("default").withName("cons1persec").scale(size + 1);
                        //scaled = true;
                        start = Instant.now();
                        log.info("since  arrival rate  predicted  {} is greater than  maximum consumed messages rate (size*poll) ,  I up scaled  by one {}",
                                prediction, (size * poll));
                    }
                } else {
                    log.info("Consumers are equal to nb partitions we can not scale up anymore");
                }
            }
            else if (prediction < ((size - 1) * poll) && (totalArrivalRate * 1000) < (size - 1) * poll && prediction > 0) {
                try (final KubernetesClient k8s = new DefaultKubernetesClient()) {
                    ServiceAccount fabric8 = new ServiceAccountBuilder().withNewMetadata().withName("fabric8").endMetadata().build();
                    k8s.serviceAccounts().inNamespace("default").createOrReplace(fabric8);
                    int replicas = k8s.apps().deployments().inNamespace("default").withName("cons1persec").get().getSpec().getReplicas();
                    if (replicas > 1) {
                        k8s.apps().deployments().inNamespace("default").withName("cons1persec").scale(replicas - 1);
                        // firstIteration = true;
                        //scaled = true;
                        //firstIteration = true;
                        start = Instant.now();
                        log.info("since   arrival rate {} is lower than max   consumption rate  with size -1 times dth, I down scaled  by one {}",
                                totalArrivalRate * 1000, ((size - 1) * poll));
                    } else {
                        log.info("Not going to  down scale since replicas already one");
                    }
                }
            }
        }
        iteration++;
        log.info("=================================:");
    }











    static void scaleDecision( Map<String, ConsumerGroupDescription> consumerGroupDescriptionMap) {
        int size = consumerGroupDescriptionMap.get(Scaler.CONSUMER_GROUP).members().size();
        log.info("Logging current consumer rates:");
        log.info("=================================:");
        for (MemberDescription memberDescription : consumerGroupDescriptionMap.get(Scaler.CONSUMER_GROUP).members()) {
            log.info("current maximum consumption rate for consumer {} is {}, ", memberDescription,
                    maxConsumptionRatePerConsumer.getOrDefault(memberDescription, 0.0f) * 1000);
        }
        log.info("=================================:");
        for (MemberDescription memberDescription : consumerGroupDescriptionMap.get(Scaler.CONSUMER_GROUP).members()) {
            long totalpoff = 0;
            long totalcoff = 0;
            long totalepoff = 0;
            long totalecoff = 0;
            for (TopicPartition tp : memberDescription.assignment().topicPartitions()) {
                totalpoff += previousPartitionToCommittedOffset.get(tp);
                totalcoff += currentPartitionToCommittedOffset.get(tp);
                totalepoff += previousPartitionToLastOffset.get(tp);
                totalecoff += currentPartitionToLastOffset.get(tp);
            }
            log.info("=================================:");
            float consumptionRatePerConsumer = (float) (totalcoff - totalpoff) / sleep;
            float arrivalRatePerConsumer = (float) (totalecoff - totalepoff) / sleep;
            if (arrivalRatePerConsumer >= consumptionRatePerConsumer) {
                // TODO do something
            }
            log.info("Current consumption rate of consumer {} is equal to {} per  seconds ", memberDescription.consumerId(),
                    consumptionRatePerConsumer * 1000);
            log.info("Current  arrival  rate to partitions of consumer {} is equal to {} per  seconds ", memberDescription.consumerId(),
                    arrivalRatePerConsumer * 1000);
            if (consumptionRatePerConsumer > maxConsumptionRatePerConsumer.getOrDefault(memberDescription, 0.0f)) {
                log.info("current consumer rate {} > max consumer rate {} swapping:", consumptionRatePerConsumer * 1000,
                        maxConsumptionRatePerConsumer.getOrDefault(memberDescription, 0.0f) * 1000);
                maxConsumptionRatePerConsumer.put(memberDescription, consumptionRatePerConsumer);
            }
            if (consumerToLag.get(memberDescription) >
                    (maxConsumptionRatePerConsumer.getOrDefault(memberDescription, 0.0f) * waitingTime))
             /*consumptionRatePerConsumer * waitingTime)*/ {
                log.info("The magic formula for consumer {} does NOT hold I am going to scale by one for now : lag {}, " +
                                "consumptionRatePerConsumer * waitingTime {} ", memberDescription.consumerId(),
                        consumerToLag.get(memberDescription), /*consumptionRatePerConsumer * waitingTime*/
                        (maxConsumptionRatePerConsumer.getOrDefault(memberDescription, 0.0f) * waitingTime));
                if (size < numberOfPartitions) {
                    log.info("Consumers are less than nb partition we can scale");
                    try (final KubernetesClient k8s = new DefaultKubernetesClient()) {
                        ServiceAccount fabric8 = new ServiceAccountBuilder().withNewMetadata().withName("fabric8").endMetadata().build();
                        k8s.serviceAccounts().inNamespace("default").createOrReplace(fabric8);
                        k8s.apps().deployments().inNamespace("default").withName("cons1persec").scale(size + 1);
                        scaled = true;
                        start = Instant.now();
                        break;
                    }
                } else {
                    log.info("Consumers are equal to nb partitions we can not scale anymore");
                }
            }
            log.info("Next consumer scale up");
            log.info("=================================:");
        }
        ///////////////////////////////////////////////////////////////////////////////////////////////////
        HashMap<MemberDescription, Long> temp = sortConsumerGroupDescriptionMap();
        // TODO ConsumerGroupDescriptionMap by descending lag with
        // TODO with respect to consumertola
        log.info("print sorted consumers by lag");
        log.info("=================================:");
        for (Map.Entry<MemberDescription, Long> memberDescription : temp.entrySet()) {
            log.info("sorted consumer id {} has the following lag {}", memberDescription.getKey().consumerId(),
                    memberDescription.getValue());
        }
        log.info("=================================:");
        if (!scaled) {
            for (MemberDescription memberDescription : temp.keySet()) {
                log.info("Begin Iteration scale down");
                log.info("=================================:");
                long totalpoff = 0;
                long totalcoff = 0;
                long totalepoff = 0;
                long totalecoff = 0;

                for (TopicPartition tp : memberDescription.assignment().topicPartitions()) {
                    totalpoff += previousPartitionToCommittedOffset.get(tp);
                    totalcoff += currentPartitionToCommittedOffset.get(tp);
                    totalepoff += previousPartitionToLastOffset.get(tp);
                    totalecoff += currentPartitionToLastOffset.get(tp);
                }

                float consumptionRatePerConsumer = (float) (totalcoff - totalpoff) / sleep;
                float arrivalRatePerConsumer = (float) (totalecoff - totalepoff) / sleep;
                log.info("Current consumption rate of consumer {} is equal to {} per  seconds ", memberDescription.consumerId(),
                        consumptionRatePerConsumer * 1000);
                log.info("Current  arrival  rate to partitions of consumer {} is equal to {} per  seconds ", memberDescription.consumerId(),
                        arrivalRatePerConsumer * 1000);
                /////////////////////////////////////////////////////////////////////////////////////////////
                if (consumptionRatePerConsumer >  maxConsumptionRatePerConsumer.getOrDefault(memberDescription, 0.0f)) {
                    log.info("current consumer rate {} > max consumer rate {} swapping:", consumptionRatePerConsumer * 1000,
                            maxConsumptionRatePerConsumer.getOrDefault(memberDescription, 0.0f) * 1000);
                    maxConsumptionRatePerConsumer.put(memberDescription, consumptionRatePerConsumer);
                }
                if(arrivalRatePerConsumer >= maxConsumptionRatePerConsumer.getOrDefault(memberDescription, 0.0f)) /*consumptionRatePerConsumer)*/ {
                    log.info("I am not going to downscale consumer {} since  arrivalRatePerConsumer >= " +
                            "consumptionRatePerConsumer", memberDescription.consumerId());
                    continue;
                }


                if (consumerToLag.get(memberDescription) < (maxConsumptionRatePerConsumer.getOrDefault(memberDescription, 0.0f)))
                    /*consumptionRatePerConsumer * waitingTime)*/ {
                      /*  log.info("The magic formula for consumer {} does hold I am going to down scale by one for now as trial",
                                memberDescription.consumerId());*/

                    log.info("The magic formula for consumer {} does hold I am going to down scale by one for now as trial, " +
                                    "lag {}, consumptionRatePerConsumer * waitingTime {} ", memberDescription.consumerId(),
                            consumerToLag.get(memberDescription), (maxConsumptionRatePerConsumer.getOrDefault(memberDescription, 0.0f) * waitingTime)
                            /*consumptionRatePerConsumer * waitingTime*/);
                    try (final KubernetesClient k8s = new DefaultKubernetesClient()) {
                        ServiceAccount fabric8 = new ServiceAccountBuilder().withNewMetadata().withName("fabric8").endMetadata().build();
                        k8s.serviceAccounts().inNamespace("default").createOrReplace(fabric8);
                        int replicas = k8s.apps().deployments().inNamespace("default").withName("cons1persec").get().getSpec().getReplicas();
                        if (replicas > 1) {
                            k8s.apps().deployments().inNamespace("default").withName("cons1persec").scale(replicas - 1);
                            // firstIteration = true;
                            scaled = true;
                            start = Instant.now();

                            // recheck is this is needed....
                            //sleep = 2 * sleep;
                            break;
                        } else {
                            log.info("Not going to scale since replicas already one");
                        }
                    }
                }
                log.info("End Iteration scale down");
                log.info("=================================:");
            }
        }
    }


    /////////////////////////////////////////////////////////////////////////////////////////
    // when downscaling delete the consumer with minimum lag
    static MemberDescription getConsumeWithLowestLag( Map<MemberDescription, Long> consumerToLag) {
        Map.Entry<MemberDescription, Long> min = null;
        for (Map.Entry<MemberDescription, Long> entry : consumerToLag.entrySet()) {
            if (min == null || min.getValue() > entry.getValue()) {
                min = entry;
            }
        }
        return min.getKey();
    }

    // when downscaling delete the consumer with minimum lag
    static  MemberDescription getConsumeWithLargestLag(Map<MemberDescription, Long> consumerToLag) {
        Map.Entry<MemberDescription, Long> max = null;
        for (Map.Entry<MemberDescription, Long> entry : consumerToLag.entrySet()) {
            if (max  == null || max.getValue() < entry.getValue()) {
                max = entry;
            }
        }
        return max.getKey();
    }


    static HashMap<MemberDescription, Long> sortConsumerGroupDescriptionMap() {
        List<Map.Entry<MemberDescription, Long>> list =
                new LinkedList<Map.Entry<MemberDescription, Long> >(consumerToLag.entrySet());
        // Sort the list
        Collections.sort(list, (o1, o2) -> (o2.getValue()).compareTo(o1.getValue()));
        // put data from sorted list to hashmap
        HashMap<MemberDescription, Long> temp = new LinkedHashMap<>();
        for (Map.Entry<MemberDescription, Long> aa : list) {
            temp.put(aa.getKey(), aa.getValue());
        }
        return temp;
}

    static void scaleDecision2(Map<String, ConsumerGroupDescription> consumerGroupDescriptionMap) {
        float totalConsumptionRate = 0;
        float totalArrivalRate = 0;
        long totallag = 0;
        int size = consumerGroupDescriptionMap.get(Scaler.CONSUMER_GROUP).members().size();
        log.info("Currently we have this number of consumers {}", size);
        for (MemberDescription memberDescription : consumerGroupDescriptionMap.get(Scaler.CONSUMER_GROUP).members()) {
            long totalpoff = 0;
            long totalcoff = 0;
            long totalepoff = 0;
            long totalecoff = 0;
            for (TopicPartition tp : memberDescription.assignment().topicPartitions()) {
                totalpoff += previousPartitionToCommittedOffset.get(tp);
                totalcoff += currentPartitionToCommittedOffset.get(tp);
                totalepoff += previousPartitionToLastOffset.get(tp);
                totalecoff += currentPartitionToLastOffset.get(tp);
            }
            float consumptionRatePerConsumer = (float) (totalcoff - totalpoff) / sleep;
            float arrivalRatePerConsumer = (float) (totalecoff - totalepoff) / sleep;

            if (arrivalRatePerConsumer >= consumptionRatePerConsumer) {
                // TODO do something
            }
            log.info("=================================:");
            if (consumptionRatePerConsumer > maxConsumptionRatePerConsumer.getOrDefault(memberDescription, 0.0f)) {
                maxConsumptionRatePerConsumer.put(memberDescription, consumptionRatePerConsumer);
            }
            totalConsumptionRate += consumptionRatePerConsumer;
            totalArrivalRate += arrivalRatePerConsumer;
            totallag += consumerToLag.get(memberDescription);
        }
        iteration++;
        log.info("totalArrivalRate {}, totalconsumptionRate {}, totallag {}",
                totalArrivalRate*1000, totalConsumptionRate*1000, totallag);
        log.info("shall we up scale totalArrivalrate {}, max  consumption rate {}",
                totalArrivalRate *1000, (size *poll* uth)/(float)SEC);

        if ((totalArrivalRate *1000) > ((size *poll * uth)/(float)SEC))  {
            if (size < numberOfPartitions) {
                log.info("Consumers are less than nb partition we can scale");
                try (final KubernetesClient k8s = new DefaultKubernetesClient()) {
                    ServiceAccount fabric8 = new ServiceAccountBuilder().withNewMetadata().withName("fabric8").endMetadata().build();
                    k8s.serviceAccounts().inNamespace("default").createOrReplace(fabric8);
                    k8s.apps().deployments().inNamespace("default").withName("cons1persec").scale(size + 1);
                    scaled = true;
                    start = Instant.now();
                  //  firstIteration = true;
                    log.info("since  arrival rate   {} is greater than  maximum consumed messages rate " +
                                    "(size*poll/SEC) *uth ,  I up scaled  by one {}",
                            totalArrivalRate * 1000, /*size *poll*/ /*totalConsumptionRate *1000*/(size *poll*uth)/(float)SEC);
                }
            } else {
                log.info("Consumers are equal to nb partitions we can not scale up anymore");
            }
        }
        else if ((totalArrivalRate *1000)  < (((size-1) *poll * dth)/(float)SEC))  {
            try (final KubernetesClient k8s = new DefaultKubernetesClient()) {
                ServiceAccount fabric8 = new ServiceAccountBuilder().withNewMetadata().withName("fabric8").endMetadata().build();
                k8s.serviceAccounts().inNamespace("default").createOrReplace(fabric8);
                int replicas = k8s.apps().deployments().inNamespace("default").withName("cons1persec").get().getSpec().getReplicas();
                if (replicas > 1) {
                    k8s.apps().deployments().inNamespace("default").withName("cons1persec").scale(replicas - 1);
                    // firstIteration = true;

                    scaled = true;
                   // firstIteration = true;
                    start = Instant.now();

                    log.info("since   arrival rate {} is lower than max   consumption rate " +
                                    " with size -1 times dth, I down scaled  by one {}",
                            totalArrivalRate * 1000, /*totalConsumptionRate *1000 */ /*size *poll*/
                            ((size-1) *poll *dth)/(float)SEC);
                } else {
                    log.info("Not going to  down scale since replicas already one");
                }
            }
        }
        log.info("quitting scale decision");
        log.info("=================================:");
    }



//////////////////////////////////////////////////////////////////


    static void scaleDecision3(Map<String, ConsumerGroupDescription> consumerGroupDescriptionMap) {
        float totalConsumptionRate = 0;
        float totalArrivalRate = 0;
        long totallag = 0;
        int size = consumerGroupDescriptionMap.get(Scaler.CONSUMER_GROUP).members().size();
        log.info("Currently we have this number of consumers {}", size);
        for (MemberDescription memberDescription : consumerGroupDescriptionMap.get(Scaler.CONSUMER_GROUP).members()) {
            long totalpoff = 0;
            long totalcoff = 0;
            long totalepoff = 0;
            long totalecoff = 0;

            for (TopicPartition tp : memberDescription.assignment().topicPartitions()) {
                totalpoff += previousPartitionToCommittedOffset.get(tp);
                totalcoff += currentPartitionToCommittedOffset.get(tp);
                totalepoff += previousPartitionToLastOffset.get(tp);
                totalecoff += currentPartitionToLastOffset.get(tp);
            }

            float consumptionRatePerConsumer = (float) (totalcoff - totalpoff) / sleep;
            float arrivalRatePerConsumer = (float) (totalecoff - totalepoff) / sleep;
            if (arrivalRatePerConsumer >= consumptionRatePerConsumer) {
                // TODO do something
            }
            if (consumptionRatePerConsumer >
                    maxConsumptionRatePerConsumer.getOrDefault(memberDescription, 0.0f)) {
                maxConsumptionRatePerConsumer.put(memberDescription, consumptionRatePerConsumer);
            }
            totalConsumptionRate += consumptionRatePerConsumer;
            totalArrivalRate += arrivalRatePerConsumer;
            totallag += consumerToLag.get(memberDescription);
        }

        log.info("=================================:");
        iteration++;
        log.info("totalArrivalRate {}, totalconsumptionRate {}, totallag {}",
                totalArrivalRate*1000, totalConsumptionRate*1000, totallag);

        log.info("shall we scale totalArrivalrate {}, current  consumption rate {}",
                totalArrivalRate *1000, /*size *poll **/totalConsumptionRate *1000);
       // if ((totalArrivalRate *1000) > (size *poll)) /*totalConsumptionRate *1000*/ {
        if ((totallag) > (size *poll * waitingTime * uth )) /*totalConsumptionRate *1000*/ {
            log.info("totallag  {}, size *poll * waitingTime * uth {}", totallag , size *poll * waitingTime *uth);
            double ratio = ((double)totallag) / ((double)(size *poll * waitingTime*uth));
            ratio = Math.ceil(ratio);
            log.info("ratio after ceil {}", ratio);
            //int needed = (int)ratio -size;
            if (size < numberOfPartitions) {
                log.info("Consumers are less than nb partition we can scale");
                try (final KubernetesClient k8s = new DefaultKubernetesClient()) {
                    ServiceAccount fabric8 =
                            new ServiceAccountBuilder().withNewMetadata().withName("fabric8").endMetadata().build();
                    k8s.serviceAccounts().inNamespace("default").createOrReplace(fabric8);
                   /* k8s.apps().deployments().inNamespace("default").withName("cons1persec").scale((int)ratio);*/
                    k8s.apps().deployments().inNamespace("default").withName("cons1persec").scale(size+1);
                    scaled = true;
                    firstIteration = true;
                    start = Instant.now();
                    log.info("since  total lag   {} is violates the SLA   (size*poll * waiting time *uth ) {} ,  I up scaled  by one",
                            totallag  ,  size *poll * waitingTime *dth);
                }
            } else {
                log.info("Consumers are equal to nb partitions we can not scale up anymore");
            }
        }
        else if (totallag  < ((size-1) *poll*waitingTime*dth)) {
            try (final KubernetesClient k8s = new DefaultKubernetesClient()) {
                ServiceAccount fabric8 = new ServiceAccountBuilder().withNewMetadata().withName("fabric8").endMetadata().build();
                k8s.serviceAccounts().inNamespace("default").createOrReplace(fabric8);
                int replicas = k8s.apps().deployments().inNamespace("default").withName("cons1persec").get().getSpec().getReplicas();

                if (replicas > 1) {
                    k8s.apps().deployments().inNamespace("default").withName("cons1persec").scale(replicas - 1);
                    scaled = true;
                    start = Instant.now();
                    firstIteration = true;
                    log.info("since   total lag  {} does not violate the SLA if I removed 1 consumer " +
                                    "(size-1) *poll*waitingTime,  I down scaled  by one {}",
                            totallag  ,(size-1) *poll*waitingTime*dth);

                } else {
                    log.info("Not going to  down scale since replicas already one");
                }
            }
        } else {
            log.info("There was no scaling action in this iteration");
        }
        log.info("=================================:");
    }
}

















