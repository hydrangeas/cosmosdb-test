using System.ComponentModel;
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
            using var blobStreamReader = new StreamReader(stream);

            var configuration = new CsvConfiguration(CultureInfo.InvariantCulture)
            {
                HasHeaderRecord = false,
                BadDataFound = null,
                MissingFieldFound = null,
            };
            using var csv = new CsvReader(reader: blobStreamReader, configuration: configuration);

            var records = csv.GetRecords<ErrorHistory>();
            var lastRecord = records?.Last();
            if (lastRecord == null)
            {
                return;
            }

            var hash = new SHA1CryptoServiceProvider().ComputeHash(Encoding.UTF8.GetBytes($"{lastRecord.ModelName}{lastRecord.SerialNumber}{lastRecord.OccurredAt}"));

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

            //var content = await blobStreamReader.ReadToEndAsync();
            //_logger.LogInformation($"C# Blob trigger function Processed blob\n Name: {name} \n Data: {content}");

            var uri = @"https://localhost:8081";
            var key = @"C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==";
            var databaseName = "DWX";
            var collectionName = "Errors";

            var client = new CosmosClient(accountEndpoint: uri, authKeyOrResourceToken: key);
            var container = client.GetContainer(databaseName, collectionName);
            var feedOptions = new QueryRequestOptions { MaxItemCount = -1 };

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
                Console.WriteLine("\nCreating item");
                ItemResponse<dynamic> response = await container.UpsertItemAsync<dynamic>(
                    errorHistory, new PartitionKey(errorHistory.ModelName));
                dynamic createdErrorHistory = response.Resource;
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
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
