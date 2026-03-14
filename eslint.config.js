import js from "@eslint/js";
import globals from "globals";
import reactPlugin from "eslint-plugin-react";
import reactHooks from "eslint-plugin-react-hooks";

export default [
  {
    ignores: ["node_modules/**", "web-dist/**"],
  },
  js.configs.recommended,
  {
    files: ["web/**/*.{js,jsx}"],
    languageOptions: {
      ecmaVersion: "latest",
      sourceType: "module",
      parserOptions: {
        ecmaFeatures: {
          jsx: true,
        },
      },
      globals: {
        ...globals.browser,
      },
    },
    settings: {
      react: {
        version: "detect",
      },
    },
  },
  reactPlugin.configs.flat.recommended,
  reactPlugin.configs.flat["jsx-runtime"],
  {
    files: ["web/**/*.{js,jsx}"],
    plugins: {
      "react-hooks": reactHooks,
    },
    rules: {
      "react/prop-types": "off",
      ...reactHooks.configs.flat.recommended.rules,
    },
  },
];
