package com.mesosphere.rendler.main;

import com.google.common.collect.ImmutableList;
import com.netflix.fenzo.*;
import com.netflix.fenzo.functions.Action1;
import com.netflix.fenzo.plugins.VMLeaseObject;
import org.apache.mesos.Protos;

import java.util.List;

/** Fenzo playground */
public class FenzoTest {
    
    public static void main(String[] args) {
        TaskScheduler taskScheduler = new TaskScheduler.Builder()
                .withLeaseRejectAction(new Action1<VirtualMachineLease>() {
                    @Override
                    public void call(VirtualMachineLease virtualMachineLease) {
                        System.out.println("Offer Rejected " + virtualMachineLease.getVMID());     
                    }
                })
                .withFitnessCalculator(new VMTaskFitnessCalculator() {
                    @Override
                    public String getName() {
                        return "fitness calculator";
                    }

                    @Override
                    public double calculateFitness(TaskRequest taskRequest, VirtualMachineCurrentState targetVM, TaskTrackerState taskTrackerState) {
                        
                        return 0;
                    }
                }).build();
        
        
        TaskRequest task = new WorkLoad("1", "group1", 200, 20000, 3, 1, 10);
        
        List<VirtualMachineLease> offers  = ImmutableList.<VirtualMachineLease>of(new VMLeaseObject(getOffer()));
        SchedulingResult schedulingResult = taskScheduler.scheduleOnce(ImmutableList.of(task), offers);
        System.out.println(schedulingResult);


    }

    private static Protos.Offer getOffer() {
        return Protos.Offer.newBuilder()
                .setId(Protos.OfferID.newBuilder().setValue("offer1").build())
                .setFrameworkId(Protos.FrameworkID.newBuilder().setValue("framework1"))
                .setHostname("Slave1")
                .setSlaveId(Protos.SlaveID.newBuilder().setValue("slave1"))
                .addAllAttributes(getAttributes()).addAllResources(getResources()).build();
    }
    
    
    private static List<? extends Protos.Attribute> getAttributes() {
        return ImmutableList.of(
                Protos.Attribute.newBuilder().setType(Protos.Value.Type.TEXT).setName("rack").setText(Protos.Value.Text.newBuilder().setValue("1")).build(),
                Protos.Attribute.newBuilder().setType(Protos.Value.Type.TEXT).setName("unit").setText(Protos.Value.Text.newBuilder().setValue("3")).build()
                );
    }

    public static Iterable<? extends Protos.Resource> getResources() {
        return ImmutableList.of(
                Protos.Resource.newBuilder().setType(Protos.Value.Type.SCALAR).setName("cpu").setScalar(getScalar(3)).build(),
                Protos.Resource.newBuilder().setType(Protos.Value.Type.SCALAR).setName("mem").setScalar(getScalar(200)).build());
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
            return null;
        }

        @Override
        public List<? extends VMTaskFitnessCalculator> getSoftConstraints() {
            return null;
        }
    }
}
