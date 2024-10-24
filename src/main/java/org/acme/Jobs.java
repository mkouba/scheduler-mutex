package org.acme;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.eclipse.microprofile.config.inject.ConfigProperty;

import io.quarkus.logging.Log;
import io.quarkus.scheduler.FailedExecution;
import io.quarkus.scheduler.Scheduled;
import io.quarkus.scheduler.Scheduled.SkipPredicate;
import io.quarkus.scheduler.ScheduledExecution;
import io.quarkus.scheduler.SuccessfulExecution;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Singleton;
import jakarta.persistence.OptimisticLockException;
import jakarta.persistence.PersistenceException;
import jakarta.transaction.Transactional;

@Singleton
public class Jobs implements SkipPredicate {

    /**
     * If a "cluster node" does not update its last check within this interval it is
     * considered "down".
     */
    private static final long LAST_CHECK_INTERVAL = 5000l;

    private static final String GUARDED_IDENTITY = "myFirst";

    private final AtomicBoolean currentNodeRunning = new AtomicBoolean();

    /**
     * Each "cluster node" should define a unique identity.
     */
    @ConfigProperty(name = "mutex.owner.identity")
    String mutexOwnerIdentity;

    /**
     * This is the "guarded" job.
     */
    @Scheduled(identity = GUARDED_IDENTITY, cron = "0/5 * * * * ?", skipExecutionIf = Jobs.class)
    void myFirstJob() throws InterruptedException {
        Log.infof("My first job started... [%s]", mutexOwnerIdentity);
        TimeUnit.SECONDS.sleep(10);
        Log.infof("My first job finished... [%s]", mutexOwnerIdentity);
    }

    /**
     * Each "cluster node" must update the last check within
     * {@link #LAST_CHECK_INTERVAL}.
     */
    @Transactional
    @Scheduled(identity = "check", every = "2s")
    void checkOwner() {
        Optional<MutexOwner> owner = MutexOwner.find("identity", mutexOwnerIdentity).singleResultOptional();
        if (owner.isEmpty()) {
            MutexOwner o = new MutexOwner();
            o.identity = mutexOwnerIdentity;
            o.lastCheck = Instant.now();
            o.persist();
        } else {
            owner.get().lastCheck = Instant.now();
        }
    }

    @Override
    public boolean test(ScheduledExecution execution) {
        if (currentNodeRunning.compareAndSet(false, true)) {
            boolean locked = false;
            try {
                locked = tryLock();
            } catch (OptimisticLockException e) {
                Log.infof("Unable to lock mutex [%s]: %s", mutexOwnerIdentity, e.getMessage());
            } catch (Exception e) {
                Log.errorf(e, "Unable to lock mutex [%s]", mutexOwnerIdentity);
            }
            if (locked) {
                return false;
            } else {
                currentNodeRunning.set(false);
            }
        } else {
            Log.infof("Skip same node [%s]", mutexOwnerIdentity);
        }
        return true;
    }

    /**
     * 
     * @param mutex
     * @return {@code true} if the lock was successful
     */
    @Transactional
    boolean tryLock() {
        Optional<Mutex> maybeMutex = Mutex.find("identity", "myFirst").singleResultOptional();
        Mutex mutex;
        if (maybeMutex.isEmpty()) {
            mutex = new Mutex();
            mutex.identity = "myFirst";
            mutex.persist();
        } else {
            mutex = maybeMutex.get();
        }
        if (mutex.lockOwner == null) {
            return doLock(mutex);
        } else if (mutex.lockOwner.equals(mutexOwnerIdentity)) {
            Log.infof("Unreleased mutex lock detected [%s]", mutexOwnerIdentity);
            return true;
        } else {
            // Verify the last check of the owner
            MutexOwner owner = MutexOwner.find("identity", mutex.lockOwner).singleResult();
            long diff = owner.lastCheck != null ? Duration.between(owner.lastCheck, Instant.now()).toMillis() : 0;
            if (owner.lastCheck == null
                    || diff > LAST_CHECK_INTERVAL) {
                // Owner is down, release the lock
                Log.infof("Mutex owner %s is down for %S seconds - relasing [%s]", mutex.lockOwner,
                        TimeUnit.MILLISECONDS.toSeconds(diff), mutexOwnerIdentity);
                return doLock(mutex);
            }
            Log.infof("Unable to lock - mutex locked by %s [%s]", mutex.lockOwner, mutexOwnerIdentity);
            return false;
        }
    }

    private boolean doLock(Mutex mutex) {
        mutex.lockOwner = mutexOwnerIdentity;
        try {
            Mutex.flush();
            Log.infof("Mutex locked [%s]", mutexOwnerIdentity);
            return true;
        } catch (PersistenceException e) {
            Log.errorf(e, "Unable to lock mutex [%s]", mutexOwnerIdentity);
            return false;
        }
    }

    void releaseOnSuccess(@Observes SuccessfulExecution ex) {
        try {
            if (release(ex.getExecution())) {
                Log.infof("Released mutex [%s]", mutexOwnerIdentity);
                currentNodeRunning.set(false);
            }
        } catch (Exception e) {
            Log.errorf(e, "Unable to release mutex [%s]", mutexOwnerIdentity);
        }
    }

    void releaseOnFailure(@Observes FailedExecution ex) {
        try {
            if (release(ex.getExecution())) {
                Log.infof("Released mutex [%s]", mutexOwnerIdentity);
                currentNodeRunning.set(false);
            }
        } catch (Exception e) {
            Log.errorf(e, "Unable to release mutex [%s]", mutexOwnerIdentity);
        }
    }

    @Transactional
    boolean release(ScheduledExecution execution) {
        if (execution.getTrigger().getId().equals(GUARDED_IDENTITY)) {
            Mutex mutex = Mutex.find("identity", "myFirst").singleResult();
            if (mutex.lockOwner.equals(mutexOwnerIdentity)) {
                mutex.lockOwner = null;
                try {
                    Mutex.flush();
                    return true;
                } catch (PersistenceException e) {
                    Log.errorf(e, "Unable to release mutex [%s]", mutexOwnerIdentity);
                }
            }
        }
        return false;
    }

}
