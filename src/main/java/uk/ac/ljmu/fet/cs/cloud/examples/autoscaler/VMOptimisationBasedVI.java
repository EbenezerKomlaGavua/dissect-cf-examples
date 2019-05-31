package uk.ac.ljmu.fet.cs.cloud.examples.autoscaler;

import hu.mta.sztaki.lpds.cloud.simulator.helpers.job.Job;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.IaaSService;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.SplittableRandom;

import hu.mta.sztaki.lpds.cloud.simulator.iaas.IaaSService;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.VirtualMachine;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.VirtualMachine.State;
import hu.mta.sztaki.lpds.cloud.simulator.iaas.constraints.ConstantConstraints;
import hu.mta.sztaki.lpds.cloud.simulator.io.Repository;
import hu.mta.sztaki.lpds.cloud.simulator.io.VirtualAppliance;



/**
 * The class applies a simple VMOptimisationBased scaling mechanism: it ensures that  VMs are create to handed all categories of executables
 * All VM processes should be optimised.
 * There should be at least a few VMs that are reused for other executables
 * The CPU and Memory should also be adjusted to accommodate few unused VMs when required.
 * @author "Gavua Ebenezer Komla, Institute of Information Technology, University of Miskolc
 *         , (c) 2019"
 */


public class VMOptimisationBasedVI extends VirtualInfrastructure {

	
	/**
	 * We keep track of how many times we found the last VM completely unused for an
	 * particular executable
	 */
	/**
	 * The number of VMs we should keep unused.
	 */
	public static final int ReservedSet = 5;
	private static final SplittableRandom rnd = new SplittableRandom(0);
	
	/**
	 * We keep track of how many times we found the last VMs completely unused for
	 * an particular executable
	 */
	private final HashMap<String, Integer> unappliedHits = new HashMap<String, Integer>();

	//private HashMap<VirtualMachine, Job> vmsWithPurpose = new HashMap<VirtualMachine, Job>();
	
	
	public VMOptimisationBasedVI(IaaSService cloud) {
		super(cloud);
		// TODO Auto-generated constructor stub
	}
	
	
	/** Variable to monitor the quantity of Jobs in the VM */
	
	private int count =0;
	/** Variable to monitor the quantity of Jobs in the VM */
	private int threshHold =10;

	private static ArrayList<String> availableJobs = new ArrayList<String>();

	public static void loadAvailablesJobs(){
		List<Job>  Jobs = JobArrivalHandler.getWaitingJobs();
				for (int i =0; i < Jobs.size(); i++) {
			availableJobs.add(Jobs.get(i).executable);
			}
	}
	
    public  void createMoreVMs() {
    	final ArrayList<VirtualMachine> vm = new ArrayList<VirtualMachine>();
  
    }
    
	
	
	/**
	 * The auto scaling mechanism that is run regularly to determine if the virtual
	 * infrastructure needs some changes. The logic is the following:
	* <ul>
	 *  <li>if we have fewer VMs than the minimum ReservedSet, we will create a
	 * VM</li>
	 * <li>if we have fewer unused VMs than the minimum  ReservedSet, we will
	 * create a VM</li>
	 * <li>if all VMs are unused, we will consider the Reserve to be completely
	 * destroyed after an hour of disuse</li>
	 * <li>if we have more VMs unused than the ReservedSet we will destroy one of
	 * the unused ones.</li>
	 * </ul>
	 */
	
	

	@SuppressWarnings("unlikely-arg-type")
	@Override
	public void tick(long fires) {
				// Regular operation, the actual "autoscaler"
		final Iterator<String> kinds = vmSetPerKind.keySet().iterator();
		
		QueueManager run = AutoScalingDemo.getQueue();
		ArrayList<String> checkQueueManager  = new ArrayList<String>();
		checkQueueManager.addAll(run.getQueue().keySet());

		//Target to test threshold
		String lotsOfJobs = "Test Threshold";

		//Begin VMOptimisation analysis
		for (String aString : availableJobs) {
		if(aString ==(lotsOfJobs)) {
		 count++;
		}else

		if (count == threshHold) {
			requestVM(aString);
			count=0;
			break;
			}
	}

	
		while (kinds.hasNext()) {
			final String kind = kinds.next();
			final ArrayList<VirtualMachine> vms = vmSetPerKind.get(kind);

			// Determining if we need a brand new kind of VM:
			if (vms.size() < ReservedSet) {
				// Not enough VMs with this kind yet, we need at least the X for each so let's
				// create one (X being the headroom defined as a constant for the class)
				requestVM(kind);
			} 
			else {
				// Let's detect the current VM utilisation pattern
				final ArrayList<VirtualMachine> unusedVMs = new ArrayList<VirtualMachine>();
				for (final VirtualMachine vm : vms) {
					if (vm.underProcessing.isEmpty() && vm.toBeAdded.isEmpty()) {
						unusedVMs.add(vm);
					}
				}
				if (unusedVMs.size() < ReservedSet) {
					// Too many VMs are used in the ReservedSet, we need to increase the VM count so new
					// tasks can already arrive for ready and unused VMs
					requestVM(kind);
				} else if (vms.size() == unusedVMs.size()) {
					// All VMs are unused, the ReservedSet might be unnecessary after all.
					Integer i = unappliedHits.get(kind);
					if (i == null) {
						unappliedHits.put(kind, 1);
					} else {
						i++;
						if (i < 20) {
							// It is not unnecessary yet, we just keep count on how many times we seen this
							// ReservedSet unused
							unappliedHits.put(kind, i);
						} else {
							// After an hour of disuse, we just drop the VMs
							unappliedHits.remove(kind);
						//	for (ArrayList<VirtualMachine> vm : vmSetPerKind.values()) {
							while (!vms.isEmpty()) {
								destroyVM(vms.get(vms.size() - 1));
							}
							kinds.remove();
							}
						//}
					}
				} else {
					// We have some of our VMs doing stuff, so we don't want the current round to
					// count towards the unnecessary hits.
					unappliedHits.remove(kind);
					if (unusedVMs.size() > ReservedSet) {
						// We have more VMs than we need at the moment, we will drop one
						destroyVM(unusedVMs.get(rnd.nextInt(unusedVMs.size())));
					}
				}
			}
		}
		boolean exit =false;
		//End of queue manager in this instance of method call
		for (ArrayList<VirtualMachine> vm : vmSetPerKind.values()) {

			for (VirtualMachine intVM : vm) {

				if(intVM.getState() == State.SUSPENDED){
					if(intVM.getVa().id ==(lotsOfJobs)){
						destroyVM(intVM);
						vmSetPerKind.remove(vm);
						
						exit =true;
						break;
				
					}
				}	
				// No VMs have tasks running, all jobs finished, we don't have to work on the
				// infrastructure anymore.
			//	for (ArrayList<VirtualMachine> vms : vmSetPerKind.values()) {
					//while (!vm.isEmpty()) {
						//destroyVM(vm.get(vm.size() - 1));
					//}
				//}
				//unsubscribe();
			
					}
			if(exit){
				break;
			}	
		}
	
	}

}
