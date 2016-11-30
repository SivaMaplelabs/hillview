package org.hiero.sketch.dataset;

import akka.actor.ActorRef;
import akka.actor.Address;
import akka.util.Timeout;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.hiero.sketch.dataset.api.*;
import org.hiero.sketch.remoting.MapOperation;
import org.hiero.sketch.remoting.SketchOperation;
import org.hiero.sketch.remoting.ZipOperation;
import rx.Observable;
import akka.pattern.Patterns;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;

/**
 * An IDataSet that is a proxy for a DataSet on a remote machine.
 */
public class RemoteDataSet<T> implements IDataSet<T> {
    private final static int TIMEOUT_MS = 1000;  // TODO: import via config file
    @NonNull
    private final ActorRef clientActor;
    @NonNull
    private final ActorRef remoteActor;

    public RemoteDataSet(
            @NonNull final ActorRef clientActor, @NonNull final ActorRef remoteActor) {
        this.clientActor = clientActor;
        this.remoteActor = remoteActor;
    }

    @Override
    public <S> Observable<PartialResult<IDataSet<S>>> map(@NonNull final IMap<T, S> mapper) {
        final Timeout timeout = new Timeout(Duration.create(TIMEOUT_MS, "milliseconds"));
        final MapOperation<T, S> mapOp = new MapOperation<T, S>(mapper);
        final Future<Object> future = Patterns.ask(this.clientActor, mapOp, TIMEOUT_MS);
        try {
            @SuppressWarnings("unchecked")
            final Observable<PartialResult<IDataSet<S>>> obs =
                (Observable<PartialResult<IDataSet<S>>>) Await.result(future, timeout.duration());
            return obs;
        } catch (final Exception e) {
            return Observable.error(e);
        }
    }

    @Override
    public <R> Observable<PartialResult<R>> sketch(@NonNull final ISketch<T, R> sketch) {
        final Timeout timeout = new Timeout(Duration.create(TIMEOUT_MS, "milliseconds"));
        final SketchOperation<T, R> sketchOp = new SketchOperation<T, R>(sketch);
        final Future<Object> future = Patterns.ask(this.clientActor, sketchOp, TIMEOUT_MS);
        try {
            @SuppressWarnings("unchecked")
            final Observable<PartialResult<R>> obs =
                    (Observable<PartialResult<R>>) Await.result(future, timeout.duration());
            return obs;
        } catch (final Exception e) {
            return Observable.error(e);
        }
    }

    @Override
    public <S> Observable<PartialResult<IDataSet<Pair<T, S>>>> zip(
            @NonNull final IDataSet<S> other) {
        if (!(other instanceof RemoteDataSet<?>)) {
            throw new RuntimeException("Unexpected type in Zip " + other);
        }
        final Timeout timeout = new Timeout(Duration.create(TIMEOUT_MS, "milliseconds"));
        final RemoteDataSet<S> rds = (RemoteDataSet<S>) other;

        // zip commands are not valid if the RemoteDataSet instances point to different
        // actor systems or different nodes.
        final Address leftAddress = this.remoteActor.path().address();
        final Address rightAddress = rds.remoteActor.path().address();
        if (!leftAddress.equals(rightAddress)) {
            throw new RuntimeException("Zip command invalid for RemoteDataSets across different servers" +
                    "| left: " + leftAddress + ", right:" + rightAddress);
        }

        final ZipOperation zip = new ZipOperation(rds.remoteActor);
        final Future<Object> future = Patterns.ask(this.clientActor, zip, TIMEOUT_MS);
        try {
            @SuppressWarnings("unchecked")
            final Observable<PartialResult<IDataSet<Pair<T, S>>>> retval =
                (Observable<PartialResult<IDataSet<Pair<T, S>>>>) Await.result(future,
                                                                               timeout.duration());
            return retval;
        } catch (final Exception e) {
            return Observable.error(e);
        }
    }
}