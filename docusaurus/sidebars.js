// @ts-check

// This runs in Node.js - Don't use client-side code here (browser APIs, JSX...)

/**
 * Creating a sidebar enables you to:
 - create an ordered group of docs
 - render a sidebar for each doc of that group
 - provide next/previous navigation

 The sidebars can be generated from the filesystem, or explicitly defined here.

 Create as many sidebars as you want.

 @type {import('@docusaurus/plugin-content-docs').SidebarsConfig}
 */
const sidebars = {
  // Manual sidebar configuration for better organization
  tutorialSidebar: [
    'intro',
    {
      type: 'category',
      label: 'Getting Started',
      items: ['tutorial/getting-started'],
    },
    'configuration',
    'datasources',
    {
      type: 'category',
      label: 'Check Types',
      items: [
        'check-types/index',
        'check-types/row-count',
        'check-types/numeric',
        'check-types/sum',
        'check-types/min',
        'check-types/max',
        'check-types/measure',
        'check-types/not-empty',
        'check-types/not-empty-pct',
        'check-types/anomaly',
      ],
    },
  ],
};

export default sidebars;
