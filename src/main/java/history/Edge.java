package history;

public class Edge<NodeType> {
    private final NodeType from;
    private final NodeType to;

    public Edge(NodeType from, NodeType to) {
        this.from = from;
        this.to = to;
    }

    public NodeType getFrom() {
        return from;
    }

    public NodeType getTo() {
        return to;
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Edge<?>) {
            Edge<?> edge = (Edge<?>) obj;
            return this == edge || from.equals(edge.from) && to.equals(edge.to);
        }
        return false;
    }
}
