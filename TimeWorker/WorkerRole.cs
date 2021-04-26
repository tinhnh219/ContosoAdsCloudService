using Microsoft.WindowsAzure;
using Microsoft.WindowsAzure.Diagnostics;
using Microsoft.WindowsAzure.ServiceRuntime;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Queue;
using Microsoft.WindowsAzure.Storage.Blob;
using ContosoAdsCommon;
using System.IO;
using System.Drawing;
using System.Drawing.Imaging;
using Microsoft.Azure;

namespace TimeWorker
{
    public class WorkerRole : RoleEntryPoint
    {
        private CloudQueue timeQueue;
        private CloudBlobContainer imagesBlobContainer;
        private ContosoAdsContext db;

        public override void Run()
        {
            Trace.TraceInformation("TimeWorker is running");
            CloudQueueMessage msg = null;

            while (true)
            {
                try
                {
                    msg = this.timeQueue.GetMessage();
                    if (msg != null)
                    {
                        ProcessTimestampQueue(msg);
                    }
                    else
                    {
                        Thread.Sleep(1000);
                    }
                }
                catch (StorageException e)
                {
                    if (msg != null && msg.DequeueCount > 5)
                    {
                        this.timeQueue.DeleteMessage(msg);
                        Trace.TraceError("Deleting poison queue item: '{0}'", msg.AsString);
                    }
                    Trace.TraceError("Exception in TimeWorker: '{0}'", e.Message);
                    Thread.Sleep(5000);
                }
            }
        }

        private void ProcessTimestampQueue(CloudQueueMessage msg)
        {
            Trace.TraceInformation("Processing queue message {0}", msg);

            var adId = int.Parse(msg.AsString);
            Ad ad = db.Ads.Find(adId);
            if (ad == null)
            {
                throw new Exception(String.Format("AdId {0} not found", adId.ToString()));
            }
            Uri blobUri = new Uri(ad.ImageURL);
            string blobName = blobUri.Segments[blobUri.Segments.Length - 1];
            CloudBlockBlob inputBlob = this.imagesBlobContainer.GetBlockBlobReference(blobName);
            string newName = Path.GetFileNameWithoutExtension(inputBlob.Name) + "new.jpg";
            CloudBlockBlob outputBlob = this.imagesBlobContainer.GetBlockBlobReference(newName);

            using (Stream input = inputBlob.OpenRead())
            using (Stream output = outputBlob.OpenWrite())
            {
                AddTimestampToImage(input, output);
                outputBlob.Properties.ContentType = "image/jpeg";
            }
            Trace.TraceInformation("Generated timestamp in blob {0}", newName);

            ad.ImageURL = outputBlob.Uri.ToString();
            db.SaveChanges();
            Trace.TraceInformation("Updated timestamp URL in database: {0}", ad.ImageURL);

            this.timeQueue.DeleteMessage(msg);
        }

        public void AddTimestampToImage(Stream input, Stream output)
        {
            var image = new Bitmap(input);
            try
            {
                Bitmap newImage = new Bitmap(image);
                using (Graphics g = Graphics.FromImage(newImage))
                {
                    Font font = new Font("Arial", 10);
                    g.DrawString(DateTime.Now.ToString(), font, Brushes.Blue, new PointF(0f, 0f));
                }
                newImage.Save(output, ImageFormat.Jpeg);
            }
            finally
            {
                if (image != null)
                {
                    image.Dispose();
                }
            }
        }

        public override bool OnStart()
        {
            // Set the maximum number of concurrent connections
            ServicePointManager.DefaultConnectionLimit = 12;

            // Read database connection string and open database.
            var dbConnString = CloudConfigurationManager.GetSetting("ContosoAdsDbConnectionString");
            db = new ContosoAdsContext(dbConnString);

            // Open storage account using credentials from .cscfg file.
            var storageAccount = CloudStorageAccount.Parse
                (RoleEnvironment.GetConfigurationSettingValue("StorageConnectionString"));

            Trace.TraceInformation("Creating images blob container");
            var blobClient = storageAccount.CreateCloudBlobClient();
            imagesBlobContainer = blobClient.GetContainerReference("images");
            if (imagesBlobContainer.CreateIfNotExists())
            {
                // Enable public access on the newly created "images" container.
                imagesBlobContainer.SetPermissions(
                    new BlobContainerPermissions
                    {
                        PublicAccess = BlobContainerPublicAccessType.Blob
                    });
            }

            Trace.TraceInformation("Creating timestamp queue");
            CloudQueueClient queueClient = storageAccount.CreateCloudQueueClient();
            timeQueue = queueClient.GetQueueReference("timestamp");
            timeQueue.CreateIfNotExists();

            Trace.TraceInformation("Storage Initialized");

            return base.OnStart();
        }

    }
}
