[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "sales_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared sales = let
  Source = Csv.Document(Web.Contents("https://github.com/MicrosoftLearning/dp-data/raw/main/sales.csv"), [Delimiter = ",", Columns = 9, Encoding = 65001, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(Source, [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"SalesOrderNumber", type text}, {"SalesOrderLineNumber", Int64.Type}, {"OrderDate", type date}, {"CustomerName", type text}, {"EmailAddress", type text}, {"Item", type text}, {"Quantity", Int64.Type}, {"UnitPrice", type number}, {"TaxAmount", type number}})
in
  #"Changed column type";
shared Parameter1 = "" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type any];
shared sales_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "d3882e81-ca32-4c69-8e0a-8b00346c2f4b"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "8743a788-2cdd-4354-bf94-a71111594303"]}[Data],
  TableNavigation = Navigation_2{[Id = "sales", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
