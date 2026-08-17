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

function AgentEvals() {
  return (
    <section className={styles.agentEvals}>
      <div className="container">
        <div className="row">
          <div className="col col--6">
            <h2>Agent Evals</h2>
            <p>
              Evaluate NL-to-SQL and BI agents the same way you define checks —
              declaratively in YAML. Compare arms, score runs, and gate releases,
              all in the same config file as your data quality checks.
            </p>
            <pre>
              <code>{`# evals.yaml
agent_variants:
  - name: baseline
    framework: pydantic_ai
    entrypoint: myapp.eval_agents.build_bi_agent
    tools: [list_views, describe_view, query, submit_answer]

eval_suites:
  - name: lookup_tool_ablation
    arms:
      - name: baseline
        agent_variant: baseline
        semantic_layer: local_sl
    metrics:
      - type: reference_value_match
      - type: llm_judge
        name: sql_soundness`}</code>
            </pre>
            <Link className="button button--outline button--secondary" to="/docs/evals">
              Explore Agent Evals
            </Link>
          </div>
          <div className="col col--6">
            <h2>Evals-As-YAML</h2>
            <p>
              The experiment design — tools, prompts, models, and test cases — lives
              entirely in YAML. Python is only needed for a one-time agent factory.
            </p>
            <div className={styles.llmFeatures}>
              <div className={styles.feature}>
                <strong>🧪 Arm Comparisons</strong>
                <p>Ablate one variable at a time across arms sharing a golden set</p>
              </div>
              <div className={styles.feature}>
                <strong>🔗 Data-Quality Attribution</strong>
                <p>Weiser's own DQ checks are wired into eval scoring — a data incident is blamed on the data, not the agent</p>
              </div>
              <div className={styles.feature}>
                <strong>⚖️ Deterministic + LLM-Judge Metrics</strong>
                <p>Zero-cost checks alongside rubric-based judging, calibrated against human labels</p>
              </div>
              <div className={styles.feature}>
                <strong>🚦 CI Regression Gating</strong>
                <p>Gate candidate runs against a baseline before they ship</p>
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
      description="Enterprise-grade data quality framework with YAML configuration, LLM-friendly design, agent evals, and advanced statistical validation"
    >
      <HomepageHeader />
      <main>
        <HomepageFeatures />
        <CodeExample />
        <AgentEvals />
      </main>
    </Layout>
  );
}
