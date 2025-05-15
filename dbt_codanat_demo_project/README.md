Welcome to your new dbt project!

### Using the starter project

Try running the following commands:
- dbt run
- dbt test


### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices


## Adding dbt connection details on local system

dbt_codanat_demo_project:
  outputs:
    dev:
      account: dbcatfo-ii99170
      database: DB_POC
      password: Hellodbttesting@121
      role: ACCOUNTADMIN
      schema: SCH_RAW
      threads: 1
      type: snowflake
      user: rohityadav121
      warehouse: COMPUTE_WH