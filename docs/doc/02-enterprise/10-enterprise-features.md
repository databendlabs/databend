---
title: Enterprise Features
---

Databend provides a single binary that includes both core and enterprise features. The core features can be accessed without a license key, while the enterprise features require either a trial or an enterprise license key. This page lists the available enterprise features.

<details>
  <summary>How do I obtain a license key?</summary>
   <p></p>
   If you're interested in obtaining a trial or enterprise license key, click this <a target="_self" href="https://databend.rs/doc/faq/license-faqs/#obtain-a-license">link</a> to find instructions on how to acquire one.<br/>

   After obtaining your license key, refer to our [License FAQs](https://databend.rs/doc/faq/license-faqs/) for guidance on how to [set your license](https://databend.rs/doc/faq/license-faqs/#set-a-license) and [verify](https://databend.rs/doc/faq/license-faqs/#verify-a-license) its validity.
</details>

| Feature                                                                             | Description                                                                                                                                                                                                                                                             |
|-------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [Data Vacuum with Fail-Safe](../14-sql-commands/00-ddl/20-table/91-vacuum-table.md) | Deep clean your storage space:<br/>- Remove orphan segment and block files. <br/>- Ensure secure data cleaning with fail-safe guarantees. <br/>- Safely preview the removal of data files using the dry-run option. |
| [Computed Columns](../14-sql-commands/00-ddl/20-table/10-ddl-create-table.md#computed-columns) | Two types of computed columns are supported: Stored and Virtual<br/>- Stored computed columns occupy additional storage space in the table and update their computed values immediately when the dependent columns are updated.<br/>- Virtual computed columns compute their values dynamically during queries and require no storage space.|