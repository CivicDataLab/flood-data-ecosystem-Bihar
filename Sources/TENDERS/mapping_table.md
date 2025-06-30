| CSV Header                        | JSON Field / Template Code                                    |
| --------------------------------- | ------------------------------------------------------------- |
| Tender ID :                       | `tenderid`                                                    |
| Tender Title :                    | `nit`                                                         |
| Work Description                  | `description`                                                 |
| Organisation Chain                | `queryString`                                                 |
| Title                             | `nit`                                                         |
| Tender Value in ₹                 | `pacamt`                                                      |
| Tender Ref No :                   | `tenderrefno`                                                 |
| Publish Date                      | `publishdate` (ms since epoch)                                |
| Bid Validity(Days)                | `offerValidity`                                               |
| Is Multi Currency Allowed For BOQ | `bidcurrency`                                                 |
| Bid Opening Date                  | template `code = bid_open_date` (field “Bid Open Date”)       |
| Tender Category                   | `tendercatid`                                                 |
| Tender Type                       | `tendertypeid`                                                |
| Form of contract                  | `proccatid`                                                   |
| Product Category                  | `deptid`                                                      |
| Allow Two Stage Bidding           | `bidPartNo`                                                   |
| Allow Preferential Bidder         | `indentFlag`                                                  |
| Payment Mode                      | template `code = payment_mode`                                |
| Status                            | `status`                                                      |
| Contract Date :                   | `createdate` (ms since epoch)                                 |
| Awarded Value                     | *no separate “awardedValue” field available; reuse* `pacamt`  |
