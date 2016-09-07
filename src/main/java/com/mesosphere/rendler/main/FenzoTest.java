package com.mesosphere.rendler.main;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.netflix.fenzo.*;
import com.netflix.fenzo.plugins.VMLeaseObject;
import org.apache.mesos.Protos;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/** Fenzo playground */
public class FenzoTest {
    
    public static void main(String[] args) {
        TaskScheduler taskScheduler = new TaskScheduler.Builder()
                .withLeaseOfferExpirySecs(20000)
                .withSingleOfferPerVM(false)
                .withLeaseRejectAction(virtualMachineLease -> {})
                .build();
 
        List<WorkLoad> taskRequests = getDSlugSilo("1");
        taskRequests.addAll(getDSlugSilo("2"));
        
        List<VirtualMachineLease> offers = getOffers();
        //taskScheduler.scheduleOnce(ImmutableList.of(), offers);
        for (WorkLoad taskRequest : taskRequests) {
            SchedulingResult schedulingResult = taskScheduler.scheduleOnce(ImmutableList.of(taskRequest), getOffers());
            System.out.println("-----------------------------------------");
            if (!schedulingResult.getResultMap().isEmpty()) {
                for(VMAssignmentResult result: schedulingResult.getResultMap().values()) {
                    List<VirtualMachineLease> leasesUsed = result.getLeasesUsed();
                    taskScheduler.getTaskAssigner().call(taskRequest, leasesUsed.get(0).hostname());
                }
            }
        }
    }
    
    private static List<WorkLoad> getDSlugSilo(String name) {
        List<WorkLoad> wl = Lists.newArrayList();
        for (int i = 0; i < 3; i++) {
            String id = String.format("service-%s-fend-%d", name, i);
            wl.add(getWorkLoad(id, name));
            id = String.format("service-%s-slug-%d", name, i);
            wl.add(getWorkLoad(id, name));
        }        
        return wl;
    }
    
    private static WorkLoad getWorkLoad(String id, String group) {
        return new WorkLoad(id, group, 1, 1, 0, 0, 0);
    }

    /** @return offers of 4 racks {1,2,3,4} with 3 units each. */
    private static List<VirtualMachineLease> getOffers() {
        return IntStream.range(1, 5).mapToObj(value -> buildRack(value))
                .flatMap(offers -> offers.stream().map(offer -> new VMLeaseObject(offer))).collect(Collectors.toList());
    } 

    private static List<Protos.Offer> buildRack(int number) {
        return IntStream.range(1, 4).mapToObj(i-> buildUnit(number, i)).collect(Collectors.toList());
    }
    
    private static Protos.Offer buildUnit(int rack, int unit) {
        return Protos.Offer.newBuilder()
                .setId(Protos.OfferID.newBuilder().setValue("offer" + UUID.randomUUID().toString()).build())
                .setFrameworkId(Protos.FrameworkID.newBuilder().setValue("framework1"))
                .setHostname(String.format("dc-r%d-u%d", rack, unit))
                .setSlaveId(Protos.SlaveID.newBuilder().setValue(String.format("slave-dc-r%d-u%d", rack, unit)))
                .addAllAttributes(getAttributes(rack, unit)).addAllResources(getResources()).build();
    }

    private static List<? extends Protos.Attribute> getAttributes(int rack, int unit) {
        return ImmutableList.of(
                Protos.Attribute.newBuilder().setType(Protos.Value.Type.TEXT).setName("rack").setText(Protos.Value.Text.newBuilder().setValue(String.valueOf(rack))).build(),
                Protos.Attribute.newBuilder().setType(Protos.Value.Type.TEXT).setName("unit").setText(Protos.Value.Text.newBuilder().setValue(String.valueOf(unit))).build()
                );
    }

    public static Iterable<? extends Protos.Resource> getResources() {
        return ImmutableList.of(
                Protos.Resource.newBuilder().setType(Protos.Value.Type.SCALAR).setName("cpus").setScalar(getScalar(1000)).build(),
                Protos.Resource.newBuilder().setType(Protos.Value.Type.SCALAR).setName("mem").setScalar(getScalar(1000)).build());
    }

    private static Protos.Value.Scalar getScalar(double i) {
        return Protos.Value.Scalar.newBuilder().setValue(i).build();
    }

//
    static class WorkLoad implements TaskRequest {

        private final String id;
        private final String taskGroupName;
        private final double cpus;
        private final double memory;
        private final double network;
        private final double disk;
        private int ports;
        
        WorkLoad(String id, String taskGroupName, double cpus, double memory, double network, double disk, int ports) {
            this.id = id;
            this.taskGroupName = taskGroupName;
            this.cpus = cpus;
            this.memory = memory;
            this.network = network;
            this.disk = disk;
            this.ports = ports;
        }

        @Override
        public String getId() {
            return id;
        }

        @Override
        public String taskGroupName() {
            return taskGroupName;
        }

        @Override
        public double getCPUs() {
            return cpus;
        }

        @Override
        public double getMemory() {
            return memory;
        }

        @Override
        public double getNetworkMbps() {
            return network;
        }

        @Override
        public double getDisk() {
            return disk;
        }

        @Override
        public int getPorts() {
            return ports;
        }

        @Override
        public List<? extends ConstraintEvaluator> getHardConstraints() {
            System.out.printf("fetching constraints for task %s%n", getId());
            return ImmutableList.of(new ConstraintEvaluator() {
                @Override public String getName() {
                    return null;
                }

                @Override public Result evaluate(TaskRequest taskRequest, VirtualMachineCurrentState targetVM, TaskTrackerState taskTrackerState) {
                    double value = siloAffinityFC.calculateFitness(taskRequest, targetVM, taskTrackerState); // soft constraint
                    return new Result(value == 1, "Doesn't fit in rack.");
                }
            });
        }

        @Override
        public List<? extends VMTaskFitnessCalculator> getSoftConstraints() {
            return null;
        }
    }
    
    private final static VMTaskFitnessCalculator siloAffinityFC = new VMTaskFitnessCalculator() {
            @Override
            public String getName() {
                return "fitness calculator";
            }

            @Override
            public double calculateFitness(TaskRequest taskRequest, VirtualMachineCurrentState targetVM, TaskTrackerState taskTrackerState) {
                System.out.println("# task: " + taskRequest.getId() + " --> host: " + targetVM.getHostname());
                // check if there's another task of the same group already deployed in this rack.

                String silo = taskRequest.taskGroupName();

                Optional<TaskTracker.ActiveTask> first = taskTrackerState.getAllRunningTasks().values().stream()
                        .filter(activeTask -> silo.equals(activeTask.getTaskRequest().taskGroupName())).findFirst();

                String offerRack = targetVM.getCurrAvailableResources().getAttributeMap().get("rack").getText().getValue();

                if (first.isPresent()) {
                    System.out.printf("Task %s silo %s %n", taskRequest.getId(), silo);

                    String alreadyAssignedRack = first.get().getTotalLease().getAttributeMap().get("rack").getText().getValue();
                    int fit = 0;
                    if (offerRack.equals(alreadyAssignedRack)) {
                        fit = 1;
                    } 
                    System.out.printf("\track %s. [%s]%n", offerRack, fit == 0 ? 'X' : 'I');
                    return fit;
                } else {
                    System.out.printf("\track %s [I] *new silo*%n", offerRack);
                    return 1;
                }
            }
        };

}
