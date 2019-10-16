package gr.ds.unipi.stpin.outputs;

public interface Output<T> extends AutoCloseable {
    void out(T data, String metaData);
}
