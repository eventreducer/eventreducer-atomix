package org.eventreducer.atomix;

import io.atomix.Atomix;
import io.atomix.coordination.DistributedLock;
import lombok.SneakyThrows;
import org.eventreducer.Lock;
import org.eventreducer.LockFactory;

public class AtomixLockFactory extends LockFactory {

    private final Atomix atomix;
    private final String prefix;

    public AtomixLockFactory(Atomix atomix, String prefix) {
        this.atomix = atomix;
        this.prefix = prefix;
    }

    @Override
    @SneakyThrows
    public Lock lock(Object lock) {
        DistributedLock distributedLock = atomix.getLock(prefix + "_" + lock.toString() + "_eventreducer_lock").get();
        distributedLock.lock().get();
        return new AtomixLock(distributedLock);
    }

    static class AtomixLock implements Lock {

        private final DistributedLock lock;

        AtomixLock(DistributedLock lock) {
            this.lock = lock;
        }

        @Override
        @SneakyThrows
        public void unlock() {
            lock.unlock().get();
        }

        @Override
        public boolean isLocked() {
            return lock.isOpen();
        }
    }

}