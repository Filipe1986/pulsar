package sn_training.util;

/**
 * This class provides the ability for an object to be registered with a JVM
 * shutdown hook. The target Shutdownable object will have its shutdown() method
 * called when the JVM exits.
 */
public final class ShutdownHook<T extends Shutdownable> implements Runnable {
    private T target = null;

    /** Constructor. */
    private ShutdownHook(T target) {
        this.target = target;
    }

    /** Run the shutdown hook for the target Shutdownable object. */
    public void run() {
        if (null != target) {
            System.out.println("Shutting down");
            target.shutdown();
        }
    }

    /**
     * Register an object to get notified during a JVM shutdown.
     * <p>
     * The type of the target must be an instantiation of the Shutdownable
     * interface. When the ShutdownHook is called by the JVM, the shutdown()
     * method of the target will be called.
     * <p>
     * The shutdown() method of the target should be judiciously coded, i.e. it
     * should be thread-safe, finish its work quickly, and should not rely upon
     * services that may have registered their own shutdown hooks.
     * <p>
     * It is possible that adding a shutdown hook may fail due to an Exception.
     * These failures will not cause an exception to be thrown from this method.
     *
     * @param target The object to register.
     */
    public static <T extends Shutdownable> void registerForShutdownHook(final T target) {
        if (null != target) {
            final ShutdownHook<T> shutdownHook = new ShutdownHook<T>(target);
            try {
                Runtime.getRuntime().addShutdownHook(new Thread(shutdownHook));
            } catch (Exception ex) {
                System.err.println("Could not add shutdown hook for target ["
                        + target.getClass().getSimpleName() + "]. Exception = " + ex);
            }
        }
    }
}