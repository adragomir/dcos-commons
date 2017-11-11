package com.mesosphere.sdk.hdfs.scheduler;

import com.mesosphere.sdk.api.EndpointUtils;
import com.mesosphere.sdk.api.types.EndpointProducer;
import com.mesosphere.sdk.config.TaskEnvRouter;
import com.mesosphere.sdk.curator.CuratorUtils;
import com.mesosphere.sdk.offer.Constants;
import com.mesosphere.sdk.offer.evaluate.placement.AndRule;
import com.mesosphere.sdk.offer.evaluate.placement.PlacementRule;
import com.mesosphere.sdk.offer.evaluate.placement.TaskTypeRule;
import com.mesosphere.sdk.offer.taskdata.EnvConstants;
import com.mesosphere.sdk.scheduler.DefaultScheduler;
import com.mesosphere.sdk.scheduler.SchedulerConfig;
import com.mesosphere.sdk.specification.*;
import com.mesosphere.sdk.specification.yaml.RawServiceSpec;
import com.mesosphere.sdk.specification.yaml.TemplateUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.*;

/**
 * HDFS Service.
 */
public class Main {
    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

    static final String SERVICE_ZK_ROOT_TASKENV = "SERVICE_ZK_ROOT";
    static final String HDFS_SITE_XML = "hdfs-site.xml";
    static final String CORE_SITE_XML = "core-site.xml";

    public static void main(String[] args) throws Exception {
        if (args.length > 0) {
            // We manually configure the pods to have additional tasktype placement rules as required for HDFS:
            new DefaultService(getBuilder(new File(args[0]))).run();
        } else {
            LOGGER.error("Missing file argument");
            System.exit(1);
        }
    }

    private static DefaultScheduler.Builder getBuilder(File pathToYamlSpecification)
            throws Exception {
        RawServiceSpec rawServiceSpec = RawServiceSpec.newBuilder(pathToYamlSpecification).build();
        File configDir = pathToYamlSpecification.getParentFile();
        SchedulerConfig schedulerConfig = SchedulerConfig.fromEnv();
        DefaultServiceSpec serviceSpec = DefaultServiceSpec.newGenerator(rawServiceSpec, schedulerConfig, configDir)
                // Used by 'zkfc' and 'zkfc-format' tasks within this pod:
                .setPodEnv("name", SERVICE_ZK_ROOT_TASKENV, CuratorUtils.getServiceRootPath(rawServiceSpec.getName()))
                .build();
        DefaultScheduler.Builder builder = DefaultScheduler
                .newBuilder(serviceSpecWithCustomizedPods(serviceSpec), schedulerConfig)
                .setRecoveryManagerFactory(new HdfsRecoveryPlanOverriderFactory())
                .setPlansFrom(rawServiceSpec);
        return builder
                .setEndpointProducer(HDFS_SITE_XML, EndpointProducer.constant(
                        renderTemplate(new File(configDir, HDFS_SITE_XML), serviceSpec.getName())))
                .setEndpointProducer(CORE_SITE_XML, EndpointProducer.constant(
                        renderTemplate(new File(configDir, CORE_SITE_XML), serviceSpec.getName())));
    }

    private static String renderTemplate(File configFile, String serviceName) {
        byte[] bytes;
        try {
            bytes = Files.readAllBytes(configFile.toPath());
        } catch (IOException e) {
            String error = String.format("Failed to read %s", configFile.getAbsolutePath());
            LOGGER.error(error, e);
            return error;
        }

        // Simulate the envvars that would be passed to a task. We want to render the config as though it was being done
        // in a task. Manually copy over a couple envvars which is included in tasks by default.
        Map<String, String> env = new HashMap<>(new TaskEnvRouter().getConfig("ALL"));
        String serviceDiscoveryDomain = System.getenv().getOrDefault("SERVICE_DISCOVERY_TLD", Constants.MESOS_DNS_TLD);
        env.put(EnvConstants.FRAMEWORK_HOST_TASKENV, EndpointUtils.toCustomDomain(serviceName, serviceDiscoveryDomain));
        env.put(EnvConstants.FRAMEWORK_NAME_TASKENV, serviceName);
        env.put("MESOS_SANDBOX", "sandboxpath");
        env.put(SERVICE_ZK_ROOT_TASKENV, CuratorUtils.getServiceRootPath(serviceName));

        String fileStr = new String(bytes, StandardCharsets.UTF_8);
        return TemplateUtils.renderMustacheThrowIfMissing(configFile.getName(), fileStr, env);
    }


    private static ServiceSpec serviceSpecWithCustomizedPods(DefaultServiceSpec serviceSpec) throws Exception {
        // Journal nodes avoid themselves and Name nodes AND satisfy passed Journal nodes placement rule.
        List<PlacementRule> journalPlacement = new ArrayList<>();
        journalPlacement.add(TaskTypeRule.avoid("journal"));
        journalPlacement.add(TaskTypeRule.avoid("name"));
        getPodSpec(serviceSpec, "journal").getPlacementRule().ifPresent(journalPlacement::add);

        // Name nodes avoid themselves and journal nodes AND satisfy passed Name nodes placement rule.
        List<PlacementRule> namePlacement = new ArrayList<>();
        namePlacement.add(TaskTypeRule.avoid("name"));
        namePlacement.add(TaskTypeRule.avoid("journal"));
        getPodSpec(serviceSpec, "name").getPlacementRule().ifPresent(namePlacement::add);

        // Data nodes avoid themselves AND satisfy passed Data nodes placement rule.
        List<PlacementRule> dataPlacement = new ArrayList<>();
        dataPlacement.add(TaskTypeRule.avoid("data"));
        getPodSpec(serviceSpec, "data").getPlacementRule().ifPresent(dataPlacement::add);

        PodSpec journal = DefaultPodSpec.newBuilder(getPodSpec(serviceSpec, "journal"))
                .placementRule(new AndRule(journalPlacement.toArray(new PlacementRule[journalPlacement.size()])))
                .build();

        PodSpec name = DefaultPodSpec.newBuilder(getPodSpec(serviceSpec, "name"))
                .placementRule(new AndRule(namePlacement.toArray(new PlacementRule[namePlacement.size()])))
                .build();

        PodSpec data = DefaultPodSpec.newBuilder(getPodSpec(serviceSpec, "data"))
                .placementRule(new AndRule(dataPlacement.toArray(new PlacementRule[dataPlacement.size()])))
                .build();

        return DefaultServiceSpec.newBuilder(serviceSpec)
                .pods(Arrays.asList(journal, name, data))
                .secret(System.getenv().getOrDefault("SERVICE_SECRET", ""))
                .build();
    }

    private static PodSpec getPodSpec(ServiceSpec serviceSpec, String podName) {
        Optional<PodSpec> match = serviceSpec.getPods().stream()
                .filter(podSpec -> podSpec.getType().equals(podName))
                .findFirst();
        if (!match.isPresent()) {
            throw new IllegalStateException(String.format(
                    "Missing required pod named '%s' in service spec", podName));
        }
        return match.get();
    }
}
