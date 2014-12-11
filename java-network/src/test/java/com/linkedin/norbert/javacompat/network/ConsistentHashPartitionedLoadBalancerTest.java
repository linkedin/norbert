package com.linkedin.norbert.javacompat.network;

import java.util.HashSet;
import java.util.Set;

import com.linkedin.norbert.javacompat.cluster.JavaNode;
import com.linkedin.norbert.javacompat.cluster.Node;
import org.junit.Assert;
import org.junit.Test;
import scala.Option;

/**
 * A unit test for the javacompat ConsistentHashPartitionedLoadBalancer.
 */
public class ConsistentHashPartitionedLoadBalancerTest {

    private static class TestEndpoint implements Endpoint {

        private final Node node;
        private final boolean canServeRequests;

        public TestEndpoint(Node node, boolean canServeRequests) {
            this.node = node;
            this.canServeRequests = canServeRequests;
        }

        @Override
        public Node getNode() {
            return node;
        }

        @Override
        public boolean canServeRequests() {
            return canServeRequests;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            TestEndpoint that = (TestEndpoint) o;

            if (!node.equals(that.node)) return false;

            return true;
        }

        @Override
        public int hashCode() {
            return node.hashCode();
        }
    }


    @Test
    public void testSingleNode() {

        // simplest test case, make sure we can find the node to route to

        Set<Endpoint> testEndpoints = new HashSet<Endpoint>();
        Set<Integer> partitionsNodeOne = new HashSet<Integer>();
        partitionsNodeOne.add(1);

        Node nodeOne = new JavaNode(1, "localhost:9000", true, partitionsNodeOne, Option.empty(), Option.empty());
        Endpoint endpointOne = new TestEndpoint(nodeOne, true);
        testEndpoints.add(endpointOne);
        ConsistentHashPartitionedLoadBalancer<Integer> loadBalancer = ConsistentHashPartitionedLoadBalancer.build(
                1,
                new HashFunction.MD5HashFunction(),
                testEndpoints,
                null
        );
        Set<Node> nodes = loadBalancer.nodesForPartitionedId(1);
        Assert.assertNotNull(nodes);
        Assert.assertEquals(1, nodes.size());
        Node node = loadBalancer.nextNode(1);
        Assert.assertNotNull(node);
    }



    @Test
    public void testTwoNodes() {
        // verify that both endpoints will get hit
        Set<Endpoint> testEndpoints = new HashSet<Endpoint>();
        Set<Integer> partitionsNodeOne = new HashSet<Integer>();
        partitionsNodeOne.add(1);

        Node nodeOne = new JavaNode(1, "localhost:9000", true, partitionsNodeOne, Option.empty(), Option.empty());
        Endpoint endpointOne = new TestEndpoint(nodeOne, true);

        Node nodeTwo = new JavaNode(2, "localhost:9001", true, partitionsNodeOne, Option.empty(), Option.empty());
        Endpoint endpointTwo = new TestEndpoint(nodeTwo, true);

        testEndpoints.add(endpointOne);
        testEndpoints.add(endpointTwo);

        ConsistentHashPartitionedLoadBalancer<Integer> loadBalancer = ConsistentHashPartitionedLoadBalancer.build(
                1,
                new HashFunction.MD5HashFunction(),
                testEndpoints,
                null
        );

        Set<Node> nodes = loadBalancer.nodesForPartitionedId(1);
        Assert.assertNotNull(nodes);
        Assert.assertEquals(1, nodes.size());

        Node resultOne = loadBalancer.nextNode(1);
        Node resultTwo = loadBalancer.nextNode(2);

        Assert.assertNotNull(resultOne);
        Assert.assertNotNull(resultTwo);

        // this was done via trial and error, there is no shortcut here
        Assert.assertEquals(nodeOne, resultOne);
        Assert.assertEquals(nodeTwo, resultTwo);
    }

    @Test
    public void testNonOverlapOfPartitions() {

    }


}
