package com.cd.stream.warning

import com.cd.stream.utils
import com.cd.stream.utils.{Logging, StringUtils}

import java.util.Properties
import javax.mail.internet.{InternetAddress, MimeMessage}
import javax.mail.{Message, Session}


object SendEmailNotification extends Logging {
  def sendEmail(userName:String, passwordMail:String,emailReceiver:String, content: String, subject: String): Unit = {
    try {
      logInfo("SendEmail/getStatus: CreateMailConnector")
      val properties = new Properties()
      properties.put("mail.smtp.auth", StringUtils.decodeBase64Text(ConfigMail.smtpAuth))
      properties.put("mail.smtp.starttls.enable", utils.StringUtils.decodeBase64Text(ConfigMail.starttlsEnable))
      properties.put("mail.smtp.host", utils.StringUtils.decodeBase64Text(ConfigMail.smtpHost))
      properties.put("mail.smtp.port", utils.StringUtils.decodeBase64Text(ConfigMail.smtpPort))
      val session = Session.getInstance(properties, new javax.mail.Authenticator {
        override def getPasswordAuthentication: javax.mail.PasswordAuthentication = {
          new javax.mail.PasswordAuthentication(utils.StringUtils.decodeBase64Text(userName), utils.StringUtils.decodeBase64Text(passwordMail))
        }
      })
      val message = new MimeMessage(session)
      message.setFrom(new InternetAddress(utils.StringUtils.decodeBase64Text(userName)))
      message.setRecipient(Message.RecipientType.TO, new InternetAddress(emailReceiver))
      message.setSubject(subject)
      message.setText(content)
      javax.mail.Transport.send(message)
      logInfo("SendEmail/getStatus: Email sent successfully")
    } catch {
      case ex: Exception => logError(s"SendEmail/getError: Failed to send email: ${ex.getMessage}")
      case _: Throwable => logError("SendEmail/getError: Sending message to kafka error")
    }
  }
}
