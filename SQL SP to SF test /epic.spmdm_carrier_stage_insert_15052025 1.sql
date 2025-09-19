CREATE OR REPLACE PROCEDURE epic.spmdm_carrier_stage_insert(
    ExecutionGUID STRING DEFAULT NULL,
    PackageName STRING DEFAULT NULL
)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    ProcName STRING DEFAULT 'epic.spmdm_carrier_stage_insert';
    StagingTableName STRING DEFAULT 'stg.tCarrier_Merge_ETL';
    ErrorMessage STRING DEFAULT '';
BEGIN
    CALL etl.spmdm_message_procedure_start(
        ExecutionGUID => ExecutionGUID,
        PackageName => PackageName,
        ProcName => ProcName
    );

    CALL etl.spmdm_message_profisee_table_row_count(
        ExecutionGUID => ExecutionGUID,
        PackageName => PackageName,
        ProcName => ProcName,
        TableName => StagingTableName
    );

    BEGIN
        INSERT INTO Profisee.stg.tCarrier_Merge_ETL
        (
            Code,
            "Name",
            SourceSystem,
            SourceSystemID,
            IsActive,
            LookupCode,
            "Type",
            SourceInsertedDate,
            SourceInsertedBy,
            SourceInactivatedDate,
            SourceUpdatedDate,
            SourceBillingCompanyID,
            SourceParentCompanyID,
            IsIssuingCompany,
            IsBillingCompany,
            IsParentCompany,
            IssuesBinders,
            AllowOnNewBusiness,
            AllowOnRenewalBusiness,
            BillingModeCode,
            ReconciliationMethodCode,
            BatchPaymentMethodCode,
            GLSubAccount,
            FATCAComplianceStatus,
            NAICCode,
            Address1,
            Address2,
            Address3,
            City,
            PostalCode,
            County,
            StateCode,
            RegionProvince,
            CountryCode,
            PhoneNumber,
            PhoneExtension,
            PhoneNumberNotes,
            FaxNumber,
            FaxNumberNotes,
            Website,
            EpicID,
            AMBestID,
            PriorCompanyID,
            ConvertedDate,
            "Is Billing Co_Epic"
        )
        SELECT  
            Code,
            "Name",
            SourceSystem,
            SourceSystemID,
            IsActive,
            LookupCode,
            "Type",
            SourceInsertedDate,
            SourceInsertedBy,
            SourceInactivatedDate,
            SourceUpdatedDate,
            SourceBillingCompanyID,
            SourceParentCompanyID,
            IsIssuingCompany,
            IsBillingCompany,
            IsParentCompany,
            IssuesBinders,
            AllowOnNewBusiness,
            AllowOnRenewalBusiness,
            BillingModeCode,
            ReconciliationMethodCode,
            BatchPaymentMethodCode,
            GLSubAccount,
            FATCAComplianceStatus,
            NAICCode,
            Address1,
            Address2,
            Address3,
            City,
            PostalCode,
            County,
            StateCode,
            RegionProvince,
            CountryCode,
            PhoneNumber,
            PhoneExtension,
            PhoneNumberNotes,
            FaxNumber,
            FaxNumberNotes,
            Website,
            EpicID,
            AMBestID,
            PriorCompanyID,
            ConversionDate,
            "Is Billing Co_Epic"
        FROM epic.vCarrier;
    EXCEPTION WHEN ERROR THEN
        ErrorMessage := 'Entity: Carrier, Source: EPIC - could not insert to staging, SQL Error: ' || ERROR_MESSAGE();
        CALL etl.spmdm_message_insert(
            Message => ErrorMessage,
            Message_Type => 'E',
            ExecutionGUID => ExecutionGUID,
            PackageName => PackageName,
            ProcName => ProcName
        );
    END;

    CALL etl.spmdm_message_profisee_table_row_count(
        ExecutionGUID => ExecutionGUID,
        PackageName => PackageName,
        ProcName => ProcName,
        TableName => StagingTableName
    );

    CALL etl.spmdm_message_procedure_finish(
        ExecutionGUID => ExecutionGUID,
        PackageName => PackageName,
        ProcName => ProcName
    );

    RETURN 'Procedure completed successfully';
END;
$$;
--End_DBShift