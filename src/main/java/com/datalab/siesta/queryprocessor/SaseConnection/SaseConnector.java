package com.datalab.siesta.queryprocessor.SaseConnection;

import com.datalab.siesta.queryprocessor.model.Events.Event;
import com.datalab.siesta.queryprocessor.model.Events.EventBoth;
import com.datalab.siesta.queryprocessor.model.GroupOccurrences;
import com.datalab.siesta.queryprocessor.model.Occurrence;
import com.datalab.siesta.queryprocessor.model.Occurrences;
import com.datalab.siesta.queryprocessor.model.Patterns.SIESTAPattern;
import com.datalab.siesta.queryprocessor.model.Utils.Utils;
import edu.umass.cs.sase.engine.EngineController;
import edu.umass.cs.sase.engine.Match;
import edu.umass.cs.sase.query.NFA;
import edu.umass.cs.sase.stream.Stream;
import net.sourceforge.jeval.EvaluationException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
@Scope(value = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class SaseConnector {


    private Utils utils;

    @Autowired
    public SaseConnector(Utils utils){
        this.utils=utils;
    }

    public List<Occurrences> evaluate(SIESTAPattern pattern, Map<Long, List<Event>> events, boolean onlyAppearances) {
        EngineController ec = this.getEngineController(pattern, onlyAppearances);
        List<Occurrences> occurrences = new ArrayList<>();
        for (Map.Entry<Long, List<Event>> e : events.entrySet()) {
            ec.initializeEngine();
            Stream s = this.getStream(new ArrayList<>(e.getValue()));
            ec.setInput(s);
            try {
                ec.runEngine();
            } catch (CloneNotSupportedException | EvaluationException exe) {
                throw new RuntimeException(exe);
            }
            if (!ec.getMatches().isEmpty()) {
                Occurrences ocs = new Occurrences();
                ocs.setTraceID(e.getKey());
                for (Match m : ec.getMatches()) {
                    ocs.addOccurrence(new Occurrence(Arrays.stream(m.getEvents()).parallel()
                            .map(x -> (SaseEvent) x)
                            .map(SaseEvent::getEventBoth)
                            .collect(Collectors.toList())));
                }
                occurrences.add(ocs);
            }
        }
        return occurrences;
    }

    public List<GroupOccurrences> evaluateGroups(SIESTAPattern pattern, Map<Integer, List<EventBoth>> events){
        EngineController ec = this.getEngineController(pattern,false);
        List<GroupOccurrences> occurrences = new ArrayList<>();
        for (Map.Entry<Integer, List<EventBoth>> e : events.entrySet()) {
            ec.initializeEngine();
            Stream s = this.getStream(new ArrayList<>(e.getValue()));
            ec.setInput(s);
            try {
                ec.runEngine();
            } catch (CloneNotSupportedException | EvaluationException exe) {
                throw new RuntimeException(exe);
            }
            if (!ec.getMatches().isEmpty()) {
                GroupOccurrences ocs = new GroupOccurrences();
                ocs.setGroupId(e.getKey());
                for (Match m : ec.getMatches()) {
                    ocs.addOccurrence(new Occurrence(Arrays.stream(m.getEvents()).parallel()
                            .map(x -> (SaseEvent) x)
                            .map(SaseEvent::getEventBoth)
                            .collect(Collectors.toList())));
                }
                occurrences.add(ocs);
            }
        }
        return occurrences;
    }

    private EngineController getEngineController(SIESTAPattern pattern, boolean onlyAppearances) {
        EngineController ec = new EngineController();
        NFAWrapper nfaWrapper = new NFAWrapper("skip-till-next-match");
        nfaWrapper.setSize(pattern.getSize());
        if (onlyAppearances) {
            nfaWrapper.setStates(pattern.getNfaWithoutConstraints());
        } else {
            nfaWrapper.setStates(pattern.getNfa());
        }
        ec.setNfa(new NFA(nfaWrapper));
        return ec;
    }


    private Stream getStream(List<Event> events) {
        Stream s = new Stream(events.size());
        List<SaseEvent> saseEvents = utils.transformToSaseEvents(events);
        s.setEvents(saseEvents.toArray(new SaseEvent[events.size()]));
        return s;
    }


}