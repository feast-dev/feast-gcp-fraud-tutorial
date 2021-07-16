gcloud functions deploy feast-update-timestamps \
--entry-point main \
--runtime python37 \
--trigger-resource feature-timestamp-schedule \
--trigger-event google.pubsub.topic.publish \
--timeout 540s

gcloud scheduler jobs create pubsub feast-update-timestamp-job \
--schedule "0 22 * * *" \
--topic feature-timestamp-schedule \
--message-body "."