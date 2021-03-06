package com.mesosphere.sdk.scheduler.plan.strategy;

import com.mesosphere.sdk.scheduler.plan.Status;
import com.mesosphere.sdk.scheduler.plan.Step;
import com.mesosphere.sdk.scheduler.plan.TestStep;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;

import static org.mockito.Mockito.when;

/**
 * This class tests the {@link ParallelStrategy}.
 */
public class ParallelStrategyTest {
    @Mock Step el0;
    @Mock Step el1;
    @Mock Step el2;

    private ParallelStrategy<Step> strategy;
    private List<Step> steps;

    @Before
    public void beforeEach() {
        MockitoAnnotations.initMocks(this);
        strategy = new ParallelStrategy<>();

        when(el0.getName()).thenReturn("step0");
        when(el1.getName()).thenReturn("step1");
        when(el2.getName()).thenReturn("step2");

        when(el0.getAsset()).thenReturn(Optional.of("step0"));
        when(el1.getAsset()).thenReturn(Optional.of("step1"));
        when(el2.getAsset()).thenReturn(Optional.of("step2"));

        when(el0.isPending()).thenReturn(true);
        when(el1.isPending()).thenReturn(true);
        when(el2.isPending()).thenReturn(true);

        steps = Arrays.asList(el0, el1, el2);
    }

    @Test
    public void testParallelExecution() {
        Assert.assertEquals(3, getCandidates().size());

        when(el0.isComplete()).thenReturn(true);
        when(el0.isPending()).thenReturn(false);
        Assert.assertEquals(2, getCandidates().size());

        when(el1.isComplete()).thenReturn(true);
        when(el1.isPending()).thenReturn(false);
        Assert.assertEquals(1, getCandidates().size());
        Assert.assertEquals(el2, getCandidates().iterator().next());

        when(el2.isComplete()).thenReturn(true);
        when(el2.isPending()).thenReturn(false);
        Assert.assertTrue(getCandidates().isEmpty());
    }

    @Test
    public void testDirtyAssetAvoidance() {
        // Can't launch because all assets are dirty
        Assert.assertTrue(strategy.getCandidates(
                steps,
                Arrays.asList(el0.getName(), el1.getName(), el2.getName())).isEmpty());

        // Can launch all now
        Assert.assertEquals(3, getCandidates().size());

        when(el0.isComplete()).thenReturn(true);
        when(el0.isPending()).thenReturn(false);
        // Can launch two because element 0 is dirty, but it's complete now.
        Assert.assertEquals(2, strategy.getCandidates(steps, Arrays.asList(el0.getName())).size());
        // Can launch el2 because el1 is dirty and el0 is complete
        Assert.assertEquals(1, strategy.getCandidates(steps, Arrays.asList(el1.getName())).size());
        Assert.assertEquals(el2, strategy.getCandidates(steps, Arrays.asList(el1.getName())).iterator().next());

        when(el1.isComplete()).thenReturn(true);
        when(el1.isPending()).thenReturn(false);
        // Can launch el2 because it's the last pending step.
        Assert.assertEquals(1, getCandidates().size());
        Assert.assertEquals(el2, getCandidates().iterator().next());

        when(el2.isComplete()).thenReturn(true);
        when(el2.isPending()).thenReturn(false);
        Assert.assertTrue(getCandidates().isEmpty());
    }

    @Test
    public void testProceedInterrupt() {
        TestStep step0 = new TestStep();
        TestStep step1 = new TestStep();
        List<Step> steps = Arrays.asList(step0, step1);

        Collection<Step> candidates = getCandidates(strategy, steps);
        Assert.assertEquals(2, candidates.size());
        Assert.assertEquals(new HashSet<>(steps), new HashSet<>(candidates));

        strategy.interrupt();
        Assert.assertTrue(getCandidates(strategy, steps).isEmpty());

        strategy.proceed();
        candidates = getCandidates(strategy, steps);
        Assert.assertEquals(2, candidates.size());
        Assert.assertEquals(new HashSet<>(steps), new HashSet<>(candidates));

        step0.setStatus(Status.COMPLETE);
        Assert.assertEquals(step1, getCandidates(strategy, steps).iterator().next());

        strategy.interrupt();
        Assert.assertTrue(getCandidates(strategy, steps).isEmpty());

        strategy.proceed();
        Assert.assertEquals(step1, getCandidates(strategy, steps).iterator().next());

        step1.setStatus(Status.COMPLETE);
        Assert.assertTrue(getCandidates(strategy, steps).isEmpty());

        strategy.interrupt();
        Assert.assertTrue(getCandidates(strategy, steps).isEmpty());
    }

    private Collection<Step> getCandidates() {
        return getCandidates(strategy, steps);
    }

    private static Collection<Step> getCandidates(Strategy<Step> strategy, Collection<Step> steps) {
        return strategy.getCandidates(steps, Collections.emptyList());
    }
}
