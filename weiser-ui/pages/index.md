---
title: Welcome to Evidence
---

<Details title='How to edit this page'>

This page can be found in your project at `/pages/index.md`. Make a change to the markdown file and save it to see the change take effect in your browser.

</Details>

```sql datasets
SELECT
  m.dataset
FROM
  metrics as m
GROUP BY
  1
LIMIT
  5000;
```

<Dropdown data={datasets} name=dataset value=dataset>
    <DropdownOption value="%" valueLabel="All Datasets"/>
</Dropdown>

```sql checks_by_name
SELECT
  m.check_name,
  DATE_TRUNC('day', m.run_time) as run_time,
  MEASURE(m.count) AS count
FROM
  metrics as m
WHERE dataset like '${inputs.dataset.value}'
AND run_time > CURRENT_TIMESTAMP() - interval '30 days'
GROUP BY
  1,
  2
LIMIT
  5000;
```

<BarChart
    data={checks_by_name}
    title="Last 30 Days Checks, {inputs.dataset.label}"
    x=run_time
    y=count
    series=check_name
/>

## What's Next?

- [Connect your data sources](settings)
- Edit/add markdown files in the `pages` folder
- Deploy your project with [Evidence Cloud](https://evidence.dev/cloud)

## Get Support

- Message us on [Slack](https://slack.evidence.dev/)
- Read the [Docs](https://docs.evidence.dev/)
- Open an issue on [Github](https://github.com/evidence-dev/evidence)
