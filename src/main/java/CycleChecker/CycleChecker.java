package CycleChecker;

import Relation.BinaryRelation;
import org.jgrapht.Graph;
import org.jgrapht.alg.cycle.CycleDetector;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.jgrapht.graph.DefaultEdge;

import java.util.*;

public class CycleChecker {
    @Deprecated
    public static boolean isCyclic(boolean[][] relation) {
        // Will find all cycles, memory out of bound
        int n = relation.length;
        System.out.println("Start Checking Cyclic, n=" + n);
        Graph<Integer, DefaultEdge> directedGraph = new DefaultDirectedGraph<Integer, DefaultEdge>(DefaultEdge.class);
        for (int i = 0; i < n; i++) {
            directedGraph.addVertex(i);
        }
        for (int i = 0; i < n; i++) {
            for (int j = 0; j < n; j++) {
                if (relation[i][j]) {
                    directedGraph.addEdge(i, j);

                }
            }
        }

        CycleDetector<Integer, DefaultEdge> detector = new CycleDetector<Integer, DefaultEdge>(directedGraph);
        return detector.detectCycles();
    }

    public static boolean topoCycleChecker(BinaryRelation relations) {
        System.out.println("Start Checking Cyclic, n=" + relations.size);

        boolean cyclic = false;
        LinkedList<Integer> stack = new LinkedList<>();
        int size = relations.size;
        int[] inDegree = new int[size];
        int tempDegree = 0;

        //根据邻接矩阵为每个点初始化入度
        for (int j = 0; j < size; j++) {
            tempDegree = 0;
            for (int i = 0; i < size; i++) {
                if (relations.get(i, j)) {
                    tempDegree++;
                }
            }
            inDegree[j] = tempDegree;
        }

        int count = 0; //判环辅助变量
        for (int i = 0; i < size; i++) {
            if (inDegree[i] == 0) { //找到入度为0的点，入栈
                stack.addFirst(i);
                inDegree[i] = -1;
            }
        }
        int curID;
        while (!stack.isEmpty()) {
            curID = stack.removeFirst();
            count++;
            for (int i = 0; i < size; i++) {
                if (relations.get(curID, i)) {
                    inDegree[i]--;
                    if (inDegree[i] == 0) {
                        stack.addFirst(i);
                        inDegree[i] = -1;
                    }
                }
            }
        }
        if (count < size) {
            cyclic = true;
            System.out.println("Detected Cyclic!");
        }
        System.out.println("Count: " + count + " Size:" + size);
        return cyclic;
    }

    static class Found {
        public boolean flag;

        public Found(boolean flag) {
            this.flag = flag;
        }
    }

    public static void dfs(BinaryRelation relations, int node, BitSet visit, Stack<Integer> cycles, Found found) {
        if (found.flag) {
            return;
        }
        int n = relations.size;
        visit.set(node, true);
        cycles.push(node);
        for (int i = 0; i < n; i++) {
            if (i == node) {
                continue;
            }
            if (relations.get(node, i)) {
                if (visit.get(i)) {
                    System.out.println("Cyclic");
//                    System.out.println(Arrays.toString(cycles.toArray()));
                    cycles.push(i);
                    found.flag = true;
                }
                dfs(relations, i, visit, cycles, found);
                if (found.flag) {
                    return;
                }
            }
        }
        cycles.pop();
        visit.set(node, false);
    }

    public static List<Integer> cutCycles(Stack<Integer> cycles) {
        int start = cycles.peek();
        int n = cycles.size();
        for (int i = 0; i < n; i++) {
            if (cycles.get(i) == start) {
                return cycles.subList(i, n);
            }
        }
        return cycles;
    }

    public static List<Integer> printCycle(BinaryRelation relations) {
        int n = relations.size;
        BitSet visit = new BitSet();
        for (int i = 0; i < n; i++) {
            visit.set(i, false);
        }
        Stack<Integer> cycles = new Stack<>();
        Found found = new Found(false);
        for (int i = 0; i < n; i++) {
            visit.set(i, true);
            cycles.push(i);
            for (int j = 0; j < n; j++) {
                if (i == j) {
                    continue;
                }
                if (relations.get(i, j)) {
                    dfs(relations, j, visit, cycles, found);
                }
                if (found.flag) {
//                    System.out.println(Arrays.toString(cycles.toArray()));
                    return cutCycles(cycles);
                }
            }
            cycles.pop();
            visit.set(i, false);
        }
        return cycles;
    }
}
