using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;
using Microsoft.Azure.Cosmos;

namespace ErrorList.Web.Pages
{
    public class IndexModel : PageModel
    {
        private readonly ILogger<IndexModel> _logger;
        private readonly IConfiguration _configuration;

        public IndexModel(ILogger<IndexModel> logger, IConfiguration configuration)
        {
            _logger = logger;
            _configuration = configuration;
        }

        public async Task OnGet()
        {
            // Cosmos DBÇÃê›íË
            var cosmosDbUri = _configuration["CosmosDbUri"];
            var cosmosDbKey = _configuration["CosmosDbKey"];
            var databaseName = "DWX";
            var collectionName = "Errors";

            // Cosmos DBÇ…ê⁄ë±
            var client = new CosmosClient(accountEndpoint: cosmosDbUri, authKeyOrResourceToken: cosmosDbKey);
            var container = client.GetContainer(databaseId: databaseName, containerId: collectionName);

            using var iterator = container.GetItemQueryIterator<ErrorHistory>(queryDefinition: new QueryDefinition("SELECT * FROM Errors"));
            while (iterator.HasMoreResults)
            {
                foreach (var errorHistory in await iterator.ReadNextAsync())
                {
                    ErrorHistories.Add(errorHistory);
                }
            }
        }

        public List<ErrorHistory> ErrorHistories { get; set; } = new List<ErrorHistory>();
        public class ErrorHistory
        {
            public string OccurredAt { get; set; } = string.Empty;
            public string TimeZone { get; set; } = string.Empty;
            public string ModelName { get; set; } = string.Empty;
            public string SerialNumber { get; set; } = string.Empty;
            public string DeviceId { get; set; } = string.Empty;
            public Int64 Code1 { get; set; }
            public Int64 Code2 { get; set; }
            public string ErrorCode { get; set; } = string.Empty;
            public string ErrorParameter { get; set; } = string.Empty;
            public string ErrorCodeParameter { get; set; } = string.Empty;
            public string ErrorMessage { get; set; } = string.Empty;
            public string MovieFileName { get; set; } = string.Empty;
            public string MillingFileName { get; set; } = string.Empty;
            public string ErrorMillFileName { get; set; } = string.Empty;

        }
    }
}
