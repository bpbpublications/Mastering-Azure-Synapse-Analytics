
--Follow the chapter 10 book steps before executing this code(Synapse databse template,fund management example)
SELECT TOP (100) [BondId]
, [TotalNumberOfBondsIssued]
, [IssuePriceAmount]
, [OriginalIssueDiscount]
, [ParValue]
, [PrincipalAmount]
, [FixedInterestRate]
, [BondCapPercentage]
, [FloatingInterestRateCalculationMethodDescription]
, [FloatingInterestRateTieToIndexName]
, [FloatingInterestCapRate]
, [FloatingInterestFloorRate]
, [CouponPaymentAmount]
, [NominalYieldPercentage]
, [DateOfIssue]
, [OfferingDate]
, [MaturityDate]
, [CallDate]
, [RedemptionDate]
, [RetractableDate]
, [MaturityPeriodInMonths]
, [CallableBondIndicator]
, [CallPriceAmount]
, [CallPremiumAmount]
, [CallProtectionPeriod]
, [CallProtectionPeriodStartDate]
, [CallProtectionPeriodEndDate]
, [ConversionPrice]
, [ConversionDate]
, [ConversionFinancialProductId]
, [ConversionRatio]
, [InsuredBondIndicator]
, [BondInsuranceProviderName]
, [SecuredBondIndicator]
, [RedeemableBondIndicator]
, [CouponBondIndicator]
, [GuaranteedBondIndicator]
, [SubjectToAmtIndicator]
, [SplitCouponBondIndicator]
, [DeferredInterestBondIndicator]
, [JointBondIndicator]
, [JunkBondIndicator]
, [PaymentInKindBondIndicator]
, [OptionalPaymentBondIndicator]
, [CollateralTrustBondIndicator]
, [AccrualBondIndicator]
, [GeneralObligationBondIndicator]
, [PreRefundedBondIndicator]
, [ListedBondIndicator]
, [IndexedBondIndicator]
, [SinkerBondIndicator]
, [SubordinatedBondIndicator]
, [DiscountedBondIndicator]
, [SinglePaymentBondIndicator]
, [PriorLienBondIndicator]
, [StrippedBondIndicator]
, [ExtendableBondIndicator]
, [InvestmentGradeBondIndicator]
, [RetractableBondIndicator]
, [PutBondIndicator]
, [ActiveBondIndicator]
, [FloaterBondIndicator]
, [SubjectToExtraordinaryRedemptionIndicator]
, [SubjectToExtraordinaryRedemptionDescription]
, [EmbeddedOptionIndicator]
, [EmbeddedOptionDescription]
, [ExemptFromFederalTaxesIndicator]
, [ExemptFromStateTaxesIndicator]
, [ExemptFromLocalTaxesIndicator]
, [RegisteredBondIndicator]
, [AdjustableBondIndicator]
, [FloatingRateBondIndicator]
, [CommodityBackedBondIndicator]
, [AdjustmentBondIndicator]
, [NonCallableBondIndicator]
, [IrredeemableBondIndicator]
, [GicsSubIndustryId]
, [InterestRateTypeId]
, [BondMaturityCategoryId]
, [BondCategoryId]
, [BondPurposeId]
, [BondDeliveryFormId]
, [BondStatusId]
, [BondPayFrequencyId]
, [BondTypeId]
, [BondGradeId]
, [BondPriorityId]
, [RedemptionFeatureId]
, [TaxExemptionTypeId]
 FROM [FundManagement_9nr].[dbo].[Bond]


/* Covid-19 ECDC cases opendata set */

/* Read parquet file */
SELECT TOP 10 *
FROM OPENROWSET (
    BULK 'https://pandemicdatalake.blob.core.windows.net/public/curated/covid-19/ecdc_cases/latest/ecdc_cases.parquet',
    FORMAT = 'parquet') as rows

/* Explicitly specify schema */
SELECT TOP 10 *
FROM OPENROWSET (
        BULK 'https://pandemicdatalake.blob.core.windows.net/public/curated/covid-19/ecdc_cases/latest/ecdc_cases.parquet',
        FORMAT = 'parquet'
    ) WITH ( date_rep date, cases int, geo_id varchar(6) ) as rows

/* New York City Taxi opendata set */

/* Query set of parquet files */
SELECT
    YEAR (tpepPickupDateTime),
    passengerCount,
    COUNT (*) AS cnt
FROM
    OPENROWSET (
        BULK 'https://azureopendatastorage.blob.core.windows.net/nyctlc/yellow/puYear=2018/puMonth=*/*.snappy.parquet',
        FORMAT='PARQUET'
    ) WITH (
        tpepPickupDateTime DATETIME2,
        passengerCount INT
    ) AS nyc
GROUP BY
    passengerCount,
    YEAR (tpepPickupDateTime)
ORDER BY
    YEAR (tpepPickupDateTime),
    passengerCount

/* Automatic schema inference */
SELECT TOP 10 *
FROM
    OPENROWSET (
        BULK 'https://azureopendatastorage.blob.core.windows.net/nyctlc/yellow/puYear=2018/puMonth=*/*.snappy.parquet',
        FORMAT='PARQUET'
    ) AS nyc

/* Query partitioned data */
SELECT
    YEAR (tpepPickupDateTime),
    passengerCount,
    COUNT (*) AS cnt
FROM
    OPENROWSET (
        BULK 'https://azureopendatastorage.blob.core.windows.net/nyctlc/yellow/puYear=*/puMonth=*/*.snappy.parquet',
        FORMAT='PARQUET'
    ) Nyc
WHERE
    nyc. filepath (1) = 2017
    AND nyc.filepath(2) IN (1, 2, 3)
    AND tpepPickupDateTime BETWEEN CAST('1/1/2017' AS datetime) AND CAST('3/31/2017' AS datetime)
GROUP BY
    passengerCount,
    YEAR (tpepPickupDateTime)
ORDER BY
    YEAR (tpepPickupDateTime),
    passengerCount.


