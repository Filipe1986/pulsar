package sn_training.util;

/**
 * Specifies the ability to perform a controlled shut down.
 */
public interface Shutdownable {
    /** Perform controlled shut down. */
    void shutdown();
    /** Cleanup after shutdown */
    void cleanup() throws Exception;
}