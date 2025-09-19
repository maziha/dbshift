{
  "requiredNodes": [
    {
      "nodeName": "SRC_ALCOBEV_PRIMARY_SALES_ACTUALS",
      "nodeType": "Source",
      "dependencies": []
    },
    {
      "nodeName": "STG_ALCOBEV_PRIMARY_SALES_ACTUALS",
      "nodeType": "Stage",
      "dependencies": [
        "SRC_ALCOBEV_PRIMARY_SALES_ACTUALS"
      ]
    },
    {
      "nodeName": "SRC_ALCOBEV_COMPANYMASTER",
      "nodeType": "Source",
      "dependencies": []
    },
    {
      "nodeName": "STG_ALCOBEV_COMPANYMASTER",
      "nodeType": "Stage",
      "dependencies": [
        "SRC_ALCOBEV_COMPANYMASTER"
      ]
    },
    {
      "nodeName": "SRC_ALCOBEV_GEOGRAPHYMASTER",
      "nodeType": "Source",
      "dependencies": []
    },
    {
      "nodeName": "STG_ALCOBEV_GEOGRAPHYMASTER",
      "nodeType": "Stage",
      "dependencies": [
        "SRC_ALCOBEV_GEOGRAPHYMASTER"
      ]
    },
    {
      "nodeName": "SRC_ALCOBEV_PRODUCTMASTER",
      "nodeType": "Source",
      "dependencies": []
    },
    {
      "nodeName": "STG_ALCOBEV_PRODUCTMASTER",
      "nodeType": "Stage",
      "dependencies": [
        "SRC_ALCOBEV_PRODUCTMASTER"
      ]
    },
    {
      "nodeName": "SRC_ALCOBEV_PRIMARY_SALES_PLAN_AOP",
      "nodeType": "Source",
      "dependencies": []
    },
    {
      "nodeName": "STG_ALCOBEV_PRIMARY_SALES_PLAN_AOP",
      "nodeType": "Stage",
      "dependencies": [
        "SRC_ALCOBEV_PRIMARY_SALES_PLAN_AOP"
      ]
    },
    {
      "nodeName": "SRC_ALCOBEV_OUTLETMASTER",
      "nodeType": "Source",
      "dependencies": []
    },
    {
      "nodeName": "STG_ALCOBEV_OUTLETMASTER",
      "nodeType": "Stage",
      "dependencies": [
        "SRC_ALCOBEV_OUTLETMASTER"
      ]
    },
    {
      "nodeName": "SRC_ALCOBEV_ACTIVATIONMASTER",
      "nodeType": "Source",
      "dependencies": []
    },
    {
      "nodeName": "STG_ALCOBEV_ACTIVATIONMASTER",
      "nodeType": "Stage",
      "dependencies": [
        "SRC_ALCOBEV_ACTIVATIONMASTER"
      ]
    },
    {
      "nodeName": "STG_AMR",
      "nodeType": "Stage",
      "dependencies": [
        "STG_ALCOBEV_ACTIVATIONMASTER"
      ]
    },
    {
      "nodeName": "STG_FCT_SALES_SUMMARY_PREP",
      "nodeType": "Stage",
      "dependencies": [
        "STG_ALCOBEV_PRIMARY_SALES_ACTUALS",
        "STG_ALCOBEV_COMPANYMASTER",
        "STG_ALCOBEV_GEOGRAPHYMASTER",
        "STG_ALCOBEV_PRODUCTMASTER",
        "STG_ALCOBEV_PRIMARY_SALES_PLAN_AOP",
        "STG_ALCOBEV_OUTLETMASTER",
        "STG_ALCOBEV_ACTIVATIONMASTER",
        "STG_AMR"
      ]
    },
    {
      "nodeName": "FCT_SALES_SUMMARY",
      "nodeType": "Fact",
      "dependencies": [
        "STG_FCT_SALES_SUMMARY_PREP"
      ]
    },
    {
      "nodeName": "V_FCT_SALES_SUMMARY",
      "nodeType": "View",
      "dependencies": [
        "FCT_SALES_SUMMARY"
      ]
    }
  ]
}