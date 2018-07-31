package startup;

import javafx.util.Pair;
import org.apache.avro.generic.GenericData;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static java.util.Comparator.comparing;

public class BatchSubRanges {
    public static Long max(Long a, Long b, Long c,Long d)
    {
        if(a>b && a>b && a>c && a>d)
        return a;
        else
            if(b>c && b>d)
            {
                return b;
            }

            else if(c>d)
                return c;
        return d;
    }


    public static List<Pair<Long,Long>> getSubRanges(List<Pair<Long,Long>> ranges) {
        List<Pair<Long,Long>> subranges= new ArrayList<>(),fullyoverlapped=new ArrayList<>(),superset=new ArrayList<>();

        Collections.sort(ranges, new Comparator<Pair<Long, Long>>() {
            @Override
            public int compare(final Pair<Long, Long> o1, final Pair<Long, Long> o2) {
                // TODO: implement your logic here

                if(o1.getKey()<=o2.getKey())
                {
                    return o1.getKey().intValue();
                }
                else
                    return o2.getKey().intValue();
            }
        });

        Collections.sort(ranges,new Comparator<Pair<Long, Long>>() {
            @Override
            public int compare(Pair<Long, Long> o1, Pair<Long, Long> o2) {

                return o1.getKey().compareTo(o2.getKey());

            }
        });

        System.out.println("SORTED RANGE " +ranges);

        System.out.println("SORTED RANGE "+ranges);
        Long maxtillnow=0L;
        for(int i=0;i<ranges.size()-1;i++)
        {
            Pair <Long, Long > p1 = ranges.get(i);
            Pair <Long, Long > p2 = ranges.get(i+1);



            boolean flag=false,flag2=false;
            if(p2.getKey()<=maxtillnow && p2.getValue()<=maxtillnow)
            {
                flag=true;

            }
            else if(p2.getKey()<=maxtillnow)
            {
                Pair <Long, Long > n = new Pair<Long,Long>(maxtillnow,p2.getValue());
                superset.add(n);
                maxtillnow=n.getValue();
                flag2=true;
            }

            else
            {
                superset.add(p1);
                maxtillnow=p1.getValue();

            }

            if(i==ranges.size()-2)
            {
                if(!flag && !flag2)
                {
                    superset.add(p2);
                }
            }
            System.out.println(maxtillnow);
        }
        return superset;
    }

    public static void main(String[] args) {

        List<Pair<Long,Long>> ranges = new ArrayList<Pair<Long,Long>>();
        Pair<Long,Long> p = new Pair<Long, Long>(1L,2L);

        ranges.add(p);
        p = new Pair<Long, Long>(5L,9L);
        ranges.add(p);
        p = new Pair<Long, Long>(4L,10L);
        ranges.add(p);
        p = new Pair<Long, Long>(6L,9L);
        ranges.add(p);
        p = new Pair<Long, Long>(6L,8L);
        ranges.add(p);
        p = new Pair<Long, Long>(3L,4L);
        ranges.add(p);
        p = new Pair<Long, Long>(9L,11L);
        ranges.add(p);
        p = new Pair<Long, Long>(7L,12L);
        ranges.add(p);

        System.out.println("INIT "+ranges);
        List<Pair<Long,Long>> superset= new ArrayList<>();
        superset=getSubRanges(ranges);
        System.out.println(superset);
    }

}
