# Spark Exam Training Quiz

This repository contains an interactive web-based quiz to practice for the Databricks Certified Associate Developer for Apache Spark exam.

The questions were extracted from `Associate-Developer-Apache-Spark-3.5 V12.65.pdf` (Spark 3.5) and `Associate-Developer-Apache-Spark V12.35.pdf` (Spark 3.0) — 308 questions total.

## How to Use Locally

1.  Clone this repository.
2.  Open the `index.html` file in your local web browser to start the quiz.

## Deploying to GitHub Pages

You can easily host this quiz on the web for free using GitHub Pages.

1.  **Push to GitHub:**
    First, commit and push the files to your GitHub repository:
    ```bash
    git add .
    git commit -m "Add quiz questions and interface"
    git push origin main
    ```

2.  **Enable GitHub Pages:**
    *   On GitHub, navigate to your repository's main page.
    *   Click on **Settings**.
    *   In the "Code and automation" section of the side menu, click on **Pages**.
    *   Under "Build and deployment", for the "Source", select **Deploy from a branch**.
    *   Under "Branch", select `main` and `/ (root)`, then click **Save**.

3.  **Access Your Quiz:**
    Wait a few minutes for the site to be built and deployed. Your quiz will be live at the URL shown on the Pages settings screen (usually `https://<your-username>.github.io/<your-repo-name>/`).
