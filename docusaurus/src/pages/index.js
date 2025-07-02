import clsx from "clsx";
import Link from "@docusaurus/Link";
import useDocusaurusContext from "@docusaurus/useDocusaurusContext";
import Layout from "@theme/Layout";
import HomepageFeatures from "@site/src/components/HomepageFeatures";

import Heading from "@theme/Heading";
import styles from "./index.module.css";

function HomepageHeader() {
  const { siteConfig } = useDocusaurusContext();
  return (
    <header className={clsx("hero hero--primary", styles.heroBanner)}>
      <div className="container">
        <Heading as="h1" className="hero__title">
          {siteConfig.title}
        </Heading>
        <p className="hero__subtitle">{siteConfig.tagline}</p>
        <div className={styles.buttons}>
          <Link
            className="button button--secondary button--lg"
            to="/docs/tutorial/getting-started"
          >
            Get Started - 5min ⏱️
          </Link>
          <Link
            className="button button--outline button--secondary button--lg"
            to="/docs/intro"
            style={{ marginLeft: '1rem' }}
          >
            Learn More
          </Link>
        </div>
      </div>
    </header>
  );
}

function CodeExample() {
  return (
    <section className={styles.codeExample}>
      <div className="container">
        <div className="row">
          <div className="col col--6">
            <h2>Simple YAML Configuration</h2>
            <p>
              Define data quality checks with intuitive YAML syntax. 
              Perfect for version control, team collaboration, and AI-assisted generation.
            </p>
            <pre>
              <code>{`# weiser-config.yaml
checks:
  - name: orders_exist
    dataset: orders
    type: row_count
    condition: gt
    threshold: 0
    
  - name: revenue_validation
    dataset: orders
    type: sum
    measure: order_amount
    condition: ge
    threshold: 10000
    filter: status = 'completed'
    
  - name: data_completeness
    dataset: customers
    type: not_empty_pct
    dimensions: [email, phone]
    condition: le
    threshold: 0.05  # Max 5% NULL`}</code>
            </pre>
          </div>
          <div className="col col--6">
            <h2>LLM-Friendly Design</h2>
            <p>
              Weiser's human-readable configuration makes it perfect for AI assistance. 
              LLMs can easily understand, generate, and modify data quality checks.
            </p>
            <div className={styles.llmFeatures}>
              <div className={styles.feature}>
                <strong>🤖 AI Code Generation</strong>
                <p>LLMs can generate Weiser configs from natural language descriptions</p>
              </div>
              <div className={styles.feature}>
                <strong>📝 Self-Documenting</strong>
                <p>YAML structure is inherently readable by both humans and AI</p>
              </div>
              <div className={styles.feature}>
                <strong>🔄 Easy Modification</strong>
                <p>AI assistants can update and refine existing configurations</p>
              </div>
              <div className={styles.feature}>
                <strong>💡 Smart Suggestions</strong>
                <p>LLMs can recommend new checks based on your data schema</p>
              </div>
            </div>
          </div>
        </div>
      </div>
    </section>
  );
}

export default function Home() {
  const { siteConfig } = useDocusaurusContext();
  return (
    <Layout
      title={`${siteConfig.title} - Data Quality Framework`}
      description="Enterprise-grade data quality framework with YAML configuration, LLM-friendly design, and advanced statistical validation"
    >
      <HomepageHeader />
      <main>
        <HomepageFeatures />
        <CodeExample />
      </main>
    </Layout>
  );
}
