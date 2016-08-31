package pubsub;

import lombok.Getter;
import rx.subjects.PublishSubject;

import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.IntStream;


public class ReactiveProducerConsumer<T> {
    private static class Bus<T> {
        @Getter
        private final PublishSubject<T> queue;

        Bus() {
            queue = PublishSubject.create();
        }

        public void publish(T t) {
            queue.onNext(t);
        }

        public void close() {
            queue.hasCompleted();
        }
    }

    private static void verify(int a, int b) {
        if (a != b) {
            throw new RuntimeException("Invalid consumption");
        }
    }

    public Bus<Integer> getInstance() {
        return new Bus<>();
    }

    public static void main(String[] args) {
        Bus<Integer> bus = new Bus<>();
        int COUNT = 10000;
        IntStream range = IntStream.range(0, COUNT);

        final ConcurrentHashMap<Integer, Integer> map = new ConcurrentHashMap<>();

        System.out.println("Subscribe");
        PublishSubject<Integer> queue = bus.queue;
        queue.asObservable().subscribe(s -> map.put(s, 1));

        System.out.println("Publish");
        range.parallel().forEach(bus::publish);
        bus.close();

        range = IntStream.range(0, COUNT);
        range.parallel().forEach(i -> verify(map.get(i), 1));
        System.out.println("Verified");
    }


}
