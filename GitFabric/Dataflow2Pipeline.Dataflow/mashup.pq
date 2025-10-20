[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "sales csv_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared #"sales csv" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "d3882e81-ca32-4c69-8e0a-8b00346c2f4b"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "8743a788-2cdd-4354-bf94-a71111594303"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "sales.csv"]}[Content],
  #"Imported CSV" = Csv.Document(#"Navigation 3", [Delimiter = ",", Columns = 7, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(#"Imported CSV", [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"SalesOrderID", Int64.Type}, {"OrderDate", type date}, {"CustomerID", Int64.Type}, {"LineItem", Int64.Type}, {"ProductID", Int64.Type}, {"OrderQty", Int64.Type}, {"LineItemTotal", type number}})
in
  #"Changed column type";
shared #"sales csv_DataDestination" = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "d3882e81-ca32-4c69-8e0a-8b00346c2f4b"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "8743a788-2cdd-4354-bf94-a71111594303"]}[Data],
  TableNavigation = Navigation_2{[Id = "sales csv", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
