resource "aws_cloudwatch_event_rule" "sync_schedule" {
  name                = "${var.project_name}-schedule"
  description         = "Trigger catalog sync on a schedule"
  schedule_expression = var.sync_schedule

  tags = {
    Project = var.project_name
  }
}

resource "aws_cloudwatch_event_target" "sync_lambda" {
  rule      = aws_cloudwatch_event_rule.sync_schedule.name
  target_id = "catalog-sync-lambda"
  arn       = aws_lambda_function.catalog_sync.arn
}

resource "aws_lambda_permission" "eventbridge" {
  statement_id  = "AllowEventBridgeInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.catalog_sync.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.sync_schedule.arn
}
