resource "aws_iam_openid_connect_provider" "github" {
  url            = "https://token.actions.githubusercontent.com"
  client_id_list = ["sts.amazonaws.com"]
}

resource "aws_iam_role" "github_deploy" {
  name = "prediction-bot-github-lightsail"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Action    = "sts:AssumeRoleWithWebIdentity"
      Principal = { Federated = aws_iam_openid_connect_provider.github.arn }
      Condition = { StringEquals = {
        "token.actions.githubusercontent.com:aud" = "sts.amazonaws.com"
        "token.actions.githubusercontent.com:sub" = "repo:rafaayud@145474430/multivenue_arbitrage_prediction_markets@1398545359:environment:lightsail"
      } }
    }]
  })
}

resource "aws_iam_role_policy" "github_deploy" {
  role = aws_iam_role.github_deploy.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = "lightsail:GetInstance"
      Resource = "arn:aws:lightsail:eu-west-1:390346501633:*"
    }, {
      Effect = "Allow"
      Action = [
        "lightsail:OpenInstancePublicPorts",
        "lightsail:CloseInstancePublicPorts"
      ]
      Resource = aws_lightsail_instance.bot.arn
    }]
  })
}

output "github_deploy_role_arn" {
  value = aws_iam_role.github_deploy.arn
}
