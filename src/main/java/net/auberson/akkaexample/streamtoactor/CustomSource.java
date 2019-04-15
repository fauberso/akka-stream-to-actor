package net.auberson.akkaexample.streamtoactor;

import akka.stream.Attributes;
import akka.stream.Outlet;
import akka.stream.SourceShape;
import akka.stream.stage.AbstractOutHandler;
import akka.stream.stage.GraphStage;
import akka.stream.stage.GraphStageLogic;

public class CustomSource extends GraphStage<SourceShape<Integer>> {
    private static final int MIN = 0;
    private static final int MAX = 999;

    public final Outlet<Integer> out = Outlet.create("Counter.out");
    private final SourceShape<Integer> shape = SourceShape.of(out);

    @Override
    public SourceShape<Integer> shape() {
        return shape;
    }

    // This is where the actual (possibly stateful) logic is created
    @Override
    public GraphStageLogic createLogic(Attributes inheritedAttributes) {
        return new GraphStageLogic(shape()) {
            // All state MUST be inside the GraphStageLogic,
            // never inside the enclosing GraphStage.
            // This state is safe to access and modify from all the
            // callbacks that are provided by GraphStageLogic and the
            // registered handlers.
            private int counter = MIN;

            {
                setHandler(out, new AbstractOutHandler() {
                    @Override
                    public void onPull() throws Exception {
                        push(out, counter);
                        counter += 1;
                        if (counter > MAX) onDownstreamFinish();
                    }
                });
            }

        };
    }

}