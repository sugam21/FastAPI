USE bfhl
-- ---------------------
-- 1. GET
-- ---------------------

-- I. Fetch Information given AccountId

SELECT 
    t.AccountId, t.Name, t.Age, t.City, t.State, t.Pincode,
    t.Id, t.CreatedDate, t.CaseNumber, t.HAN, t.BillAmount, t.Status,
    p.HAN, p.`Policy Name`
    
FROM(
    SELECT 
        a.AccountId, a.Name, a.Age, a.City, a.State, a.Pincode,
        c.Id, c.CreatedDate, c.CaseNumber, c.HAN, c.BillAmount, c.Status
    FROM bfhl.accounts as a
    LEFT JOIN bfhl.claims as c
    ON a.AccountId = c.AccountId) as t
LEFT JOIN bfhl.policies as p
ON t.HAN = p.HAN
WHERE t.AccountId = "0012j00000GjoGnAAJ";



-- ---------------------
-- 2. POST
-- ---------------------

-- ** I. Insert a Record into Accounts table
INSERT INTO bfhl.accounts (AccountId, Name, Age, City, State, Pincode)
Values ("1234567","Sugam", 21, "Pune", "Maharashtra", 412115)

--check
SELECT * FROM bfhl.accounts WHERE AccountId="1234567"

-- ** II. Insert a Record into Policies table
INSERT INTO bfhl.policies (HAN, `Policy Name`)
VALUES ("12345678", "test Policy")

--check
SELECT * FROM bfhl.policies where han="12345678" limit 1;

-- ** III. Insert a Record into Claims table
INSERT INTO bfhl.claims (Id, CreatedDate, CaseNumber, HAN, BillAmount, Status, AccountId)
VALUES ("987654321", CURRENT_TIMESTAMP,"asdfghjk","12345",123456,"Paid",12345678);

--check
SELECT * FROM bfhl.claims WHERE Id="987654321" LIMIT 1;

-- ---------------------
-- 3. DELETE
-- ---------------------

-- ** I. Delete a Record from Accounts table
DELETE FROM bfhl.accounts as a
WHERE a.AccountId = "1234567";


-- ** II. Delete a Record from Policies table
DELETE FROM bfhl.policies as p
WHERE p.HAN = "12345678";


-- ** III. Delete a Record from Claims table
DELETE FROM bfhl.claims as c
WHERE Id = "987654321";


-- ---------------------
-- 3. PUT
-- ---------------------

-- ** I. Modify a Record of Accounts table
UPDATE bfhl.accounts
SET Name = "John"
WHERE AccountId = "1234567";

-- ** II. Modify a Record of Policies table
UPDATE bfhl.policies
SET `Policy Name` = "another test policy"
WHERE HAN="12345678"

--check
SELECT * FROM bfhl.policies where han="12345678" limit 1;

-- ** III. Modify a Record of Claims table
UPDATE bfhl.claims
SET BillAmount=999
WHERE Id="987654321"

--check
SELECT * FROM bfhl.claims WHERE Id="987654321" LIMIT 1;


-- To Track Changes being made in the table, we need to create triggers