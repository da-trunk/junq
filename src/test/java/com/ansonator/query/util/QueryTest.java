package com.ansonator.query.util;

import static com.ansonator.query.Query.*;
import static org.assertj.core.api.Assertions.assertThat;

import com.ansonator.query.Query;
import com.ansonator.query.cell.Aggregator;
import com.ansonator.query.row.Row;
import com.ansonator.test.random.RepeatingRandomizer;
import com.google.common.collect.ImmutableList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.context.annotation.Configuration;

@SpringBootTest(
    classes = {QueryTest.Config.class},
    webEnvironment = WebEnvironment.NONE)
@Log4j2
public class QueryTest {

  @Configuration
  public static class Config {}

  @Data
  @AllArgsConstructor(staticName = "of")
  @NoArgsConstructor
  public static class Participant {
    @Data
    @AllArgsConstructor(staticName = "of")
    static class Speed {
      private long miles;
      private long hours;

      public static Speed staticMerge(Speed a, Speed b) {
        return new Speed(a.miles + b.miles, a.hours + b.hours);
      }

      public void merge(Speed other) {
        miles += other.miles;
        hours += other.hours;
      }

      @Override
      public String toString() {
        return String.format("%d/%d", miles, hours);
      }
    }
    ;

    private int id;
    private String species;
    private String name;
    private boolean registered;
    private Speed speed;

    @Override
    public String toString() {
      return String.format("(%d, %s, %s, %s, %s)", id, species, name, registered, speed);
    }

    static class Randomizer extends RepeatingRandomizer<Participant> {
      private RepeatingRandomizer<String> animal =
          RepeatingRandomizer.createFromFaker(3, faker -> faker.animal().name());

      Randomizer(int poolSize) {
        super(poolSize);
      }

      @Override
      public Participant get() {
        Participant.Speed speed = new Participant.Speed(random.nextInt(100), random.nextInt(100));
        return Participant.of(
            random.nextInt(5),
            animal.getRandomValue(),
            faker.artist().name(),
            random.nextInt(10) > 5 ? true : false,
            speed);
      }

      public Stream<Participant> generate() {
        return Stream.generate(this::getRandomValue);
      }
    }
  }

  @Data
  @AllArgsConstructor(staticName = "of")
  @NoArgsConstructor
  public static class Species {
    private String species;
    private String color;

    @Override
    public String toString() {
      return String.format("(%s, %s)", species, color);
    }
  }

  private static List<Participant> data =
      new Participant.Randomizer(10).generate().limit(100).collect(Collectors.toList());

  @Test
  public void orderBy() {
    List<Row> actual1 =
        Query.from(data.stream(), Participant.class)
            .orderBy("id", "name")
            .peek(row -> log.debug("ordered = {}", row))
            .list();
    List<Row> actual2 =
        Query.from(data.stream(), Participant.class)
            .orderBy(
                Comparator.comparing((Row row) -> (Integer) row.get(0))
                    .thenComparing(row -> (String) row.get(2)))
            .list();
    List<Row> actual3 =
        Query.from(data.stream(), Participant.class)
            .orderBy(
                Comparator.comparing(row -> (Integer) row.get(0)),
                Comparator.comparing(row -> (String) row.get(2)))
            .list();
    List<Integer> expected =
        data.stream().map(Participant::getId).sorted().collect(Collectors.toList());
    assertThat(actual1.stream().map(row -> (Integer) row.get(0)))
        .containsExactlyElementsOf(expected);
    assertThat(actual2.stream().map(row -> (Integer) row.get(0)))
        .containsExactlyElementsOf(expected);
    assertThat(actual3.stream().map(row -> (Integer) row.get(0)))
        .containsExactlyElementsOf(expected);
  }

  @Test
  public void where() throws NoSuchFieldException, SecurityException {
    List<Participant> actual =
        Query.from(data.stream(), Participant.class)
            .peek(row -> log.debug("from = {}", row))
            .where(isTrue("registered").and(isEqual("species", "wolf")))
            .stream(Participant.class, Participant::new)
            .peek(item -> log.debug("participant = {}", item))
            .collect(Collectors.toList());
    List<Participant> expected =
        data.stream()
            .filter(t -> t.getSpecies().equals("wolf") && t.isRegistered())
            .collect(Collectors.toList());
    assertThat(actual).containsExactlyElementsOf(expected);
  }

  @Test
  public void join() {
    RepeatingRandomizer<String> color =
        RepeatingRandomizer.createFromFaker(5, Faker -> Faker.color().name());
    List<Species> speciesData =
        data.stream()
            .map(Participant::getSpecies)
            .distinct()
            .map(species -> Species.of(species, color.getRandomValue()))
            .limit(2)
            .peek(species -> log.debug("species = {}", species))
            .collect(Collectors.toList());

    Query query =
        from(data.stream(), Participant.class)
            .peek(row -> log.debug("from = {}", row))
            .join(from(speciesData.stream(), Species.class))
            .using("species")
            .peek(row -> log.debug("join = {}", row));
    assertThat(query.getSelectedColumns().keySet())
        .containsExactly("id", "species", "name", "registered", "speed", "color");

    List<Row> actual1 = query.list();
    Set<String> expected =
        speciesData.stream().map(Species::getSpecies).collect(Collectors.toSet());
    assertThat(actual1.stream().map(row -> query.get(row, "species")).distinct())
        .containsExactlyInAnyOrderElementsOf(expected);

    Query actual2 =
        from(data.stream(), Participant.class)
            .join(from(speciesData.stream(), Species.class))
            .on("species", "right_species");
    assertThat(actual2.getSelectedColumns().keySet())
        .containsExactly("id", "species", "name", "registered", "speed", "right_species", "color");
    assertThat(actual2.select("id", "species", "name", "registered", "speed", "color").list())
        .isEqualTo(actual1);
  }

  @Test
  public void groupBy() {
    List<Row> actual =
        Query.from(data.stream(), Participant.class)
            .select("id", "species", "registered", "speed")
            .where(isEqual("species", "tortoise"), isEqual("registered", false))
            .groupBy("id", Aggregator.of("speed", Participant.Speed::merge))
            .orderBy("id")
            .list();
    assertThat(actual).hasSize(2);
    assertThat(actual.get(0))
        .isEqualTo(Row.of(0, "tortoise", false, Participant.Speed.of(54272, 38912)));
    assertThat(actual.stream().map(row -> row.get(3)).collect(Collectors.toList()))
        .containsOnly(Participant.Speed.of(74397, 50453), Participant.Speed.of(54272, 38912));
  }

  @Test
  public void groupByAndWhere() {
    List<Row> actual =
        Query.from(data.stream(), Participant.class)
            .select("id", "species", "registered")
            .where(isTrue("registered"))
            .groupBy("id", Aggregator.of("speed", Participant.Speed::merge))
            .orderBy("species")
            .list();
    List<Row> expected =
        data.stream()
            .filter(Participant::isRegistered)
            .collect(Collectors.groupingBy(Participant::getId, Collectors.toSet()))
            .values()
            .stream()
            .flatMap(set -> set.stream())
            .map(row -> new Object[] {row.getId(), row.getSpecies(), row.isRegistered()})
            .map(Row::new)
            .collect(Collectors.toList());
    assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);
  }

  @Test
  public void everything() {
    RepeatingRandomizer<String> color =
        RepeatingRandomizer.createFromFaker(5, Faker -> Faker.color().name());
    List<Species> speciesData =
        data.stream()
            .map(Participant::getSpecies)
            .distinct()
            .map(species -> Species.of(species, color.getRandomValue()))
            .limit(2)
            .collect(Collectors.toList());

    Query actual =
        Query.from(data.stream(), Participant.class)
            .select("id", "species", "registered")
            .join(from(speciesData.stream(), Species.class))
            .using("species")
            .where(isTrue("registered"))
            .groupBy("id", Aggregator.of("speed", Participant.Speed::merge))
            .orderBy("species");
    List<Row> expected = ImmutableList.of(Row.of(4, "raccoon", true, "grey"));
    assertThat(actual.list()).containsExactlyElementsOf(expected);
  }
}
