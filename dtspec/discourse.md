# Introducing dtspec

Hello dbt community!  I'm using this forum to introduce a new framework
for testing data transformations: [dtspec](https://github.com/inside-track/dtspec).
One topic that has come up in several dbt slack conversations and other
[discourse posts](https://discourse.getdbt.com/t/testing-with-fixed-data-set/564/4)
is how difficult it can be to write tests that show how the output of a data transformation
may behave given some variation with its inputs.  dtspec is designed to simplify the process
of building test data and asserting how it should be transformed.

dtspec is a very new project.  We've implemented it at my company,
InsideTrack, to test many of our dbt models.  I'm reaching out to the
dbt community to gather feedback on whether this could be a useful
tool for other data shops.

## jaffle_shop

I've set up a basic proof-of-concept [dbt jaffle_shop
project](https://github.com/gnilrets/jaffle_shop-dtspec/tree/dtspec)
that demonstrates how dtspec can be used in dbt projects.  The
interested reader is encouraged to review the [spec
file](https://github.com/gnilrets/jaffle_shop-dtspec/blob/dtspec/dtspec/spec.yml)
for that project, and even take it for a test drive locally.  Try
tweaking some of the models or expectations to see what happens when
the expectations are not actually met by the transformations.

One of the more amazing things that happened while I was writing the
spec for the `jaffle_shop` project was that _I was wrong (gasp!)_
about how I thought a certain model behaved.  When I wrote the test
case, I wrote it the way I thought the transformation worked, and
dtspec alerted me that the expectation was not being met.  I was then
able to dig into the model SQL more to figure out where my assumptions
were wrong.  I then fixed the test case and moved on.  **This is the
real power of dtspec!** Data transformations can get very complex.
Different developers, both new and seasoned, are going to have
different understandings about how the models function.  If, based on
our necessarily limited understanding, we modify the SQL in a way that
breaks some known edge case, dtspec will alert us.

## Basic concepts

With dtspec, a user writes a **spec.yaml** file describing the
expected behavior of the data transformations they wish to test.
dtspec uses this spec file to generate data that is then loaded into
an empty test data warehouse.  dbt (or really any other data
transformation system) is the run in that test warehouse.  The outputs of
the transformations (e.g., dbt models) are then extracted out of the
data warehouse.  dtspec then compares the actual results with the
expected results defined in the spec file.  Any discrepancies are
reported to the user.

This spec file contains a few key components, detailed below, with some
examples from the [jaffle_shop POC](https://github.com/gnilrets/jaffle_shop-dtspec/blob/dtspec/dtspec/test.py).

### identifiers
A single dbt run in a modern data warehouse can be
time consuming, even if the amount of data being transformed is small.
dtspec is designed to minimize the number times that dbt needs to be
run in order to be tested (in most situations, dbt would only need to
be run once).  dtspec accomplishes this by collecting all of the data
described in the cases and scenarios and stacking them on top of each
other.  Identifiers are used to declare which columns uniquely
identify records as belonging to a particular case.  See the [dtspec
README](https://github.com/inside-track/dtspec#hello-world-with-multiple-test-cases)
for more details on the topic.  In a lot of situations, we can get
away with a generic identifier for the `id` columns:
```yaml
identifiers:
  - identifier: generic
    attributes:
      - field: id
        generator: unique_integer
```

### sources
The sources in a spec file describe the data transformation inputs.  These
map directly to dbt sources.  The user can specify which columns can be used
to uniquely identify records within a case by defining an `identifier_map`:
```yaml
sources:
  - source: raw_customers
    identifier_map:
      - column: id
        identifier:
          name: generic
          attribute: id

  - source: raw_orders
    identifier_map:
      - column: id
        identifier:
          name: generic
          attribute: id
      - column: user_id
        identifier:
          name: generic
          attribute: id
```

### targets
These are the outputs of the data transformations.  In dbt, these
are known as models:
```yaml
targets:
  - target: dim_customers
    identifier_map:
      - column: customer_id
        identifier:
          name: generic
          attribute: id
```

### factories
Factories describe how to generate data for the
sources to be used as inputs for data transformations.  A single
factory can be, and usually is, composed of multiple sources that are
in some way related to each other (e.g., foreign key relationships).
Factories can be combined with other factories and tweaked for
specific test cases, thereby providing a flexible mechanism for
describing different data scenarios.
```yaml
factories:
  - factory: CustomerWithOrderAndPayment
    data:
      - source: raw_customers
        table: |
          | id    | first_name |
          | -     | -          |
          | cust1 | Kelly      |

      - source: raw_orders
        table: |
          | id     | user_id |
          | -      | -       |
          | order1 | cust1   |

      - source: raw_payments
        table: |
          | id       | order_id |
          | -        | -        |
          | payment1 | order1   |
```

### scenarios and cases

Scenarios and cases are the heart of dtspec.  Scenarios are
collections of cases that share a common base factory and typically
describe a single data target/model.  Cases describe the expected
output of a transformation, given some inputs (inherited from the
scenario, and often tweaked a bit).

```yaml
scenarios:
  - scenario: Building dim_customers
    factory:
      parents:
        # This factory is used by default in all of the cases that belong to this scenario.
        - CustomerWithOrderAndPayment

    cases:
      - case: populating number of orders
        factory:
          data:
            - source: raw_orders
              table: |
                | id     | user_id |
                | -      | -       |
                | order1 | cust1   |
                | order2 | cust1   |
                | order3 | cust1   |

        expected:
          data:
            - target: dim_customers
              table: |
                | customer_id | number_of_orders |
                | -           | -                |
                | cust1       | 3                |
```

### Testing output

The output of a run of dtspec (with a test case failure) looks something like this
```bash
Asserting Building dim_customers: target has data PASSED
Asserting Building dim_customers: populating number of orders FAILED
DataFrame.iloc[:, 1] are different

DataFrame.iloc[:, 1] values are different (100.0 %)
[left]:  [2]
[right]: [3]
Actual:
  customer_id number_of_orders
0       cust1                2
Expected:
  customer_id number_of_orders
0       cust1                3

Asserting Building dim_customers: populating most recent order date PASSED
Asserting Building dim_customers: populating customer lifetime value PASSED
Asserting Building dim_customers: unknown payment methods are not ignored when populating customer lifetime value PASSED
Asserting Building fct_orders: target has data PASSED
Asserting Building fct_orders: populating the right payment column PASSED
Asserting Building fct_orders: unknown payment methods still show up in totals, but nowhere else PASSED
Asserting Building fct_orders: multiple payments for the same order PASSED
```


## Comparison with other kinds of dbt tests

dtspec is **not** meant to replace the schema or data tests that are
currently baked in to dbt.  dtspec is meant to supplement these tests
and enhance overall test coverage.  The dbt schema and data tests are
tremendously valuable for ensuring the quality of production data.
However, they lack in being able to test hypothetical data situations
or complex, record specific, transformation logic.  dtspec is mean to
fill those gaps.

## How we're using dbt and dtspec at InsideTrack

At InsideTrack, we're using dtspec to run over 150 tests against 50
models in a Redshift data warehouse.

For organizational purposes, we've split up our `spec.yml` file into
multiple files and then process them with Jinja to knit them back
together into a single spec file (and thus we get all the other great
things that come along with Jinja).  Running all of the tests takes
about 5 minutes, and we've built our spec parser a way to run
just specific models and tests to speed up develoment iterations.

We've also got scripts to copy our production source data schemas into
a small Redshift test cluster.  That test cluster is used by our CI
system to run dtspec tests after every commit and prior to every
deploy.

We're also toying with the idea of migrating our warehouse from
Redshift to Snowflake.  In principle, we should be able to perform
that migration without modifying any of the dtspec files.  We'll
certainly have to adjust some of the model SQL to accomplish this
migration, but once we can get all of our dtspec tests passing, we'll
have excellent confidence that the migration will be successful.

## Where to learn more

Please check out both the [main dtspec github repo](https://github.com/inside-track/dtspec)
and the [jaffle_shop POC](https://github.com/gnilrets/jaffle_shop-dtspec/blob/dtspec/dtspec/test.py).

Comment on this post, or reach out to me on dbt slack if you have any questions!
