import re


# Define the split function
def split_by_items_whitespace_agnostic(content_full, item_numbers, item_type_mapping):
    compressed_chars = []
    orig_to_comp = []
    for idx, ch in enumerate(content_full):
        if not ch.isspace():
            compressed_chars.append(ch.lower())
            orig_to_comp.append(idx)
    compressed = "".join(compressed_chars)

    item_ranges = {}
    for item in item_numbers:
        desc = item_type_mapping.get(item, "")
        raw = f"item{item}{desc}"
        pattern_cmp = re.sub(r"\s+", "", raw).lower()
        m = re.search(re.escape(pattern_cmp), compressed)
        if m:
            item_ranges[item] = [m.start(), None]

    sorted_items = sorted(item_ranges.items(), key=lambda kv: kv[1][0])
    for (it, (st, _)), (nxt, (nst, _)) in zip(sorted_items, sorted_items[1:]):
        item_ranges[it][1] = nst
    if sorted_items:
        last = sorted_items[-1][0]
        item_ranges[last][1] = len(compressed)

    result = {}
    for item, (cstart, cend) in item_ranges.items():
        orig_start = orig_to_comp[cstart]
        orig_end = orig_to_comp[cend - 1] + 1
        result[item] = content_full[orig_start:orig_end].strip()

    return result


# Test string
content = """Item 5.03 Amendments to Articles of Incorporation or Bylaws; Change
in Fiscal Year.
The disclosure contained in
Item 5.07 of this Report is incorporated by reference in this Item 5.03.
Item 5.07 Submission of Matters to a Vote of Security Holders.
On April 30, 2025 and May 1, 2025, ClimateRock (the “Company”)
held an extraordinary general meeting of shareholders (the “Meeting”). At the Meeting, the following proposals were
considered and acted upon by the shareholders of the Company:
(a) a proposal to amend the
Company’s amended and restated memorandum and articles of association (the “Articles”) to extend the date by
which the Company has to consummate an initial Business Combination from May 2, 2025 to November 2, 2025 (or such earlier date as determined
by the Company’s board of directors in its sole discretion) (the “Extension Amendment Proposal” and such amendment
to the Articles, the “Extension Amendment”);
(b) a proposal to amend the
Articles to eliminate the limitation that the Company may not redeem Public Shares to the extent that such redemption would result in
the Company having net tangible assets (as determined in accordance with Rule 3a51-1(g)(1) of the Securities Exchange Act of 1934,
as amended) of less than $5,000,001 (the “Redemption Limitation”) in order to allow the Company to redeem Public Shares
irrespective of whether such redemption would exceed the Redemption Limitation (the “Redemption Limitation Amendment Proposal”and such amendment, together with the Extension Amendment, the “Articles Amendment”); and
(c)
a
proposal to approve the adjournment of the Meeting to a later date or dates, if necessary, to permit further solicitation and vote of
proxies in the event that there are insufficient votes for, or otherwise in connection with, the approval of any of the foregoing proposals
(the “Adjournment Proposal”).
The
number of votes cast for or against, as well as the number of abstentions as to each proposal, are set forth below.
1.
Extension Amendment Proposal
For
Against
Abstain
2,836,541
543,267
0
Accordingly,
the Extension Amendment Proposal was approved.
2.
Redemption Limitation Amendment Proposal
For
Against
Abstain
2,686,268
407,800
285,740
Accordingly,
the Redemption Limitation Amendment Proposal was approved.
3.
Adjournment Proposal
For
Against
Abstain
2,836,541
543,267
0
Shareholders
holding 2,058,545 Public Shares exercised their right to redeem such Public Shares for a pro rata portion of the funds in the Trust Account.
The final per share redemption amount is currently being calculated.  The Company has estimated it to be approximately $12.17 per
share and will file an amended Current Report on Form 8-K to disclose the final amount if it is materially different from the estimated
amount. As a result, approximately $25.06 million will be removed from the Trust Account to pay such holders (the “Meeting Redemptions”).
Following the Meeting Redemptions, there will be 406,678 Public Shares issued and outstanding.
The
Company filed the Articles Amendment with the Cayman Islands Registrar of Companies on May 2, 2025. A copy of the Articles Amendment is
attached hereto as Exhibit 3.1, and is incorporated by reference.
Item 9.01 Financial Statements and Exhibits.
(d) Exhibits
Exhibit No.
Description
3.1
An Amendment to the Amended and Restated Memorandum and Articles of Association of the Company
104
Cover Page Interactive Data File (embedded within the Inline XBRL document).
"""

# Define items and mapping
item_numbers = ["5.03", "5.07", "9.01"]
item_type_mapping = {
    "1.01": "Entry into a Material Definitive Agreement",
    "1.02": "Termination of a Material Definitive Agreement",
    "1.03": "Bankruptcy or Receivership",
    "1.04": "Mine Safety - Reporting of Shutdowns and Patterns of Violations",
    "1.05": "Material Cybersecurity Incidents",
    "2.01": "Completion of Acquisition or Disposition of Assets",
    "2.02": "Results of Operations and Financial Condition",
    "2.03": "Creation of a Direct Financial Obligation or an Obligation under an Off-Balance Sheet Arrangement",
    "2.04": "Triggering Events That Accelerate or Increase a Direct Financial Obligation or an Obligation",
    "2.05": "Costs Associated with Exit or Disposal Activities",
    "2.06": "Material Impairments",
    "3.01": "Notice of Delisting or Failure to Satisfy a Continued Listing Rule or Standard; Transfer of Listing",
    "3.02": "Unregistered Sales of Equity Securities",
    "3.03": "Material Modification to Rights of Security Holders",
    "4.01": "Changes in Registrant's Certifying Accountant",
    "4.02": "Non-Reliance on Previously Issued Financial Statements or a Related Audit Report",
    "5.01": "Changes in Control of Registrant",
    "5.02": "Departure of Directors or Certain Officers; Election of Directors; Appointment of Certain Officers",
    "5.03": "Amendments to Articles of Incorporation or Bylaws; Change in Fiscal Year",
    "5.04": "Temporary Suspension of Trading Under Registrant's Employee Benefit Plans",
    "5.05": "Amendments to the Registrant's Code of Ethics, or Waiver of a Provision of the Code of Ethics",
    "5.06": "Change in Shell Company Status",
    "5.07": "Submission of Matters to a Vote of Security Holders",
    "5.08": "Shareholder Director Nominations",
    "6.01": "ABS Informational and Computational Material",
    "6.02": "Change of Servicer or Trustee",
    "6.03": "Change in Credit Enhancement or Other External Support",
    "6.04": "Failure to Make a Required Distribution",
    "6.05": "Securities Act Updating Disclosure",
    "7.01": "Regulation FD Disclosure",
    "8.01": "Other Events",
    "9.01": "Financial Statements and Exhibits",
}

# Run the split function
split_contents = split_by_items_whitespace_agnostic(
    content, item_numbers, item_type_mapping
)

# Print results
for it, text in split_contents.items():
    print(f"===== Item {it} =====")
    print(text)
    print("\n")
