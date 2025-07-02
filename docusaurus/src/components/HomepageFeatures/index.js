import clsx from "clsx";
import Heading from "@theme/Heading";
import styles from "./styles.module.css";

const FeatureList = [
  {
    title: "YAML-Based Configuration",
    Img: require("@site/static/img/weiser-yaml.png").default,
    description: (
      <>
        Define your data quality checks with simple, human-readable YAML. No
        complex code required - just describe what you want to validate and
        Weiser handles the rest.
      </>
    ),
  },
  {
    title: "LLM-Friendly Design",
    Img: require("@site/static/img/llms.png").default,
    description: (
      <>
        Designed for the AI era. Large Language Models can easily understand and
        generate Weiser configurations, making it perfect for AI-assisted data
        quality management and automated check generation.
      </>
    ),
  },
  {
    title: "Enterprise-Ready Scale",
    Img: require("@site/static/img/weiser-enterprise.png").default,
    description: (
      <>
        From startup analytics to enterprise data warehouses. Supports
        PostgreSQL, Snowflake, Cube semantic layer, and scales to handle
        millions of records with advanced statistical analysis and anomaly
        detection.
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
