using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using CsvHelper;
using CsvHelper.Configuration;
using CsvHelper.Configuration.Attributes;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;

namespace ErrorList.Functions
{
    public class Function1
    {
        private readonly ILogger<Function1> _logger;

        public Function1(ILogger<Function1> logger)
        {
            _logger = logger;
        }

        [Function(nameof(Function1))]
        public async Task Run([BlobTrigger("samples-workitems/{name}", Connection = "ErrorHistoriesStorage")] Stream stream, string name)
        {
            // データの読み込み
            using var blobStreamReader = new StreamReader(stream);

            // CSVの設定
            var configuration = new CsvConfiguration(CultureInfo.InvariantCulture)
            {
                HasHeaderRecord = false,
                BadDataFound = null,
                MissingFieldFound = null,
            };
            // CSVの読み込み
            using var csv = new CsvReader(reader: blobStreamReader, configuration: configuration);

            // CSVのレコードを取得
            var records = csv.GetRecords<ErrorHistory>();
            // 最後のレコードを取得
            var lastRecord = records?.Last();
            if (lastRecord == null)
            {
                return;
            }

            // 最後のレコードのハッシュ値を計算
            var hash = SHA1.HashData(Encoding.UTF8.GetBytes($"{lastRecord.ModelName}{lastRecord.SerialNumber}{lastRecord.OccurredAt}"));

            // 最後のレコードをログに出力
            _logger.LogInformation($"C# Blob trigger function Processed blob\n Name: {name} \n Data:"
            + $"\n  OccurredAt       : {lastRecord.OccurredAt.Trim()}"
            + $"\n  TimeZone         : {lastRecord.TimeZone.Trim()}"
            + $"\n  ModelName        : {lastRecord.ModelName.Trim()}"
            + $"\n  SerialNumber     : {lastRecord.SerialNumber.Trim()}"
            + $"\n  Code1            : {lastRecord.Code1}"
            + $"\n  Code2            : {lastRecord.Code2}"
            + $"\n  ErrorCode        : {lastRecord.ErrorCode.Trim()}"
            + $"\n  ErrorParameter   : {lastRecord.ErrorParameter.Trim()}"
            + $"\n  MovieFileName    : {lastRecord.MovieFileName.Trim().Replace("\"", "")}"
            + $"\n  MillingFileName  : {lastRecord.MillingFileName.Trim().Replace("\"", "")}"
            + $"\n  ErrorMillFileName: {lastRecord.ErrorMillFileName.Trim().Replace("\"", "")}"
            + $"\n  GetHashCode()    : {hash}");

            // Cosmos DBの設定
            var cosmosDbUri = Environment.GetEnvironmentVariable("CosmosDbUri");
            var cosmosDbKey = Environment.GetEnvironmentVariable("CosmosDbKey");
            var databaseName = "DWX";
            var collectionName = "Errors";

            // Cosmos DBに接続
            var client = new CosmosClient(accountEndpoint: cosmosDbUri, authKeyOrResourceToken: cosmosDbKey);
            var container = client.GetContainer(databaseId: databaseName, containerId: collectionName);

            try
            {
                // Dynamic Object
                dynamic errorHistory = new
                {
                    id = hash,
                    OccurredAt = lastRecord.OccurredAt.Trim(),
                    TimeZone = lastRecord.TimeZone.Trim(),
                    ModelName = lastRecord.ModelName.Trim(),
                    SerialNumber = lastRecord.SerialNumber.Trim(),
                    Code1 = lastRecord.Code1,
                    Code2 = lastRecord.Code2,
                    ErrorCode = lastRecord.ErrorCode.Trim(),
                    ErrorParameter = lastRecord.ErrorParameter.Trim(),
                    MovieFileName = lastRecord.MovieFileName.Trim().Replace("\"", ""),
                    MillingFileName = lastRecord.MillingFileName.Trim().Replace("\"", ""),
                    ErrorMillFileName = lastRecord.ErrorMillFileName.Trim().Replace("\"", ""),
                };
                _logger.LogInformation("Creating an item..");
                // Cosmos DBにデータを登録/更新
                ItemResponse<dynamic> response = await container.UpsertItemAsync<dynamic>(
                    errorHistory, new PartitionKey(errorHistory.ModelName));
                dynamic createdErrorHistory = response.Resource;
                _logger.LogInformation($"Created item in database with id: {createdErrorHistory.id}");
            }
            catch (Exception e)
            {
                _logger.LogError($"Error: {e.Message}");
            }
        }

        public class ErrorHistory
        {
            [Index(0)]
            public string OccurredAt { get; set; } = string.Empty;
            [Index(1)]
            public string TimeZone { get; set; } = string.Empty;
            [Index(2)]
            public string ModelName { get; set; } = string.Empty;
            [Index(3)]
            public string SerialNumber { get; set; } = string.Empty;
            [Index(4)]
            public Int64 Code1 { get; set; }
            [Index(5)]
            public Int64 Code2 { get; set; }
            [Index(6)]
            public string ErrorCode { get; set; } = string.Empty;
            [Index(7)]
            public string ErrorParameter { get; set; } = string.Empty;
            [Index(8)]
            public string MovieFileName { get; set; } = string.Empty;
            [Index(9)]
            public string MillingFileName { get; set; } = string.Empty;
            [Index(10)]
            public string ErrorMillFileName { get; set; } = string.Empty;

        }
    }
}
