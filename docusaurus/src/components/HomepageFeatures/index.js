import clsx from "clsx";
import Heading from "@theme/Heading";
import styles from "./styles.module.css";

const FeatureList = [
  {
    title: "YAML-Based Configuration",
    Img: require("@site/static/img/weiser-yaml.png").default,
    description: (
      <>
        Define your data quality checks and agent evals with simple,
        human-readable YAML. No complex code required - just describe what you
        want to validate and Weiser handles the rest. The declarative format is
        LLM-friendly, so large language models can easily read and generate
        Weiser configurations.
      </>
    ),
  },
  {
    title: "Agent Evals",
    Img: require("@site/static/img/llms.png").default,
    description: (
      <>
        Built for data-powered agents. Evaluate NL-to-SQL and BI agents with the
        same declarative config as your checks - compare arms, score runs, and
        gate releases. Weiser's own data quality checks are wired into eval
        scoring, so when a data incident breaks an answer, blame lands on the
        data, not the agent.
      </>
    ),
  },
  {
    title: "Enterprise-Ready Scale",
    Img: require("@site/static/img/weiser-enterprise.png").default,
    description: (
      <>
        Supports PostgreSQL, MySQL, Databricks, Snowflake, BigQuery, Cube, and
        agent frameworks like Pydantic AI and Strands (more to come), scaling to
        handle millions of records with advanced statistical analysis and
        anomaly detection.
      </>
    ),
  },
];

function Feature({ Svg, Img, title, description }) {
  return (
    <div className={clsx("col col--4")}>
      <div className="text--center">
        {Svg && <Svg className={styles.featureSvg} role="img" />}
        {Img && <img src={Img} className={styles.featureSvg} alt={title} />}
      </div>
      <div className="text--center padding-horiz--md">
        <Heading as="h3">{title}</Heading>
        <p>{description}</p>
      </div>
    </div>
  );
}

export default function HomepageFeatures() {
  return (
    <section className={styles.features}>
      <div className="container">
        <div className="row">
          {FeatureList.map((props, idx) => (
            <Feature key={idx} {...props} />
          ))}
        </div>
      </div>
    </section>
  );
}
