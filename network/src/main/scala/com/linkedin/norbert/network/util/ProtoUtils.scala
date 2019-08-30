package com.linkedin.norbert
package network
package util

import com.google.protobuf.ByteString
import logging.Logging
import java.lang.reflect.{Field, Constructor}

/**
 * A wrapper for converting from byte[] <-> ByteString. Protocol buffers makes unnecessary
 * defensive copies at each conversion, and this class encapsulates logic using reflection
 * to bypass those.
 */
object ProtoUtils extends Logging {
  private val ByteStringClass = classOf[ByteString]
  // Protobuf 2.6 has protected class LiteralByteString
  private val LiteralByteStringClass: Class[ByteString] = try {
    val c = classOf[ByteString].getClassLoader().loadClass("com.google.protobuf.LiteralByteString")
    c match {
      case c2: Class[ByteString] => c2
      case _ => throw new ClassCastException
    }
  } catch {
    case ex: ClassNotFoundException => null
    case ex: Exception =>
      log.warn(ex, "Cannot eliminate a copy when converting a byte[] to a ByteString, failed to find LiteralByteString")
      null
  }

  private val byteStringConstructor: Constructor[ByteString] = try {
    var c:Constructor[ByteString] = null
    if(LiteralByteStringClass != null) {
      // Protobuf 2.6
      c = LiteralByteStringClass.getDeclaredConstructor(classOf[Array[Byte]])
    }else{
      // Protobuf 2.4
      c = classOf[ByteString].getDeclaredConstructor(classOf[Array[Byte]])
    }
    c.setAccessible(true)
    c
  } catch {
    case ex: Exception =>
      log.warn(ex, "Cannot eliminate a copy when converting a byte[] to a ByteString")
      null
  }

  private val byteStringField: Field = try {
    var f:Field = null
    if(LiteralByteStringClass != null) {
      // Protobuf 2.6
      f = LiteralByteStringClass.getDeclaredField("bytes")
    }else{
      // Protobuf 2.4
      f = classOf[ByteString].getDeclaredField("bytes")
    }
    f.setAccessible(true)
    f
  } catch {
    case ex: Exception =>
      log.warn(ex, "Cannot eliminate a copy when converting a ByteString to a byte[]")
      null
  }

  def byteArrayToByteString(byteArray: Array[Byte], avoidByteStringCopy: Boolean): ByteString = {
    if (avoidByteStringCopy)
      fastByteArrayToByteString(byteArray)
    else
      slowByteArrayToByteString(byteArray)
  }

  def byteStringToByteArray(byteString: ByteString, avoidByteStringCopy: Boolean): Array[Byte] = {
    if (avoidByteStringCopy)
      fastByteStringToByteArray(byteString)
    else
      slowByteStringToByteArray(byteString)
  }

  private final def fastByteArrayToByteString(byteArray: Array[Byte]): ByteString = {
    if (byteStringConstructor != null)
      try {
        byteStringConstructor.newInstance(byteArray)
      } catch {
        case ex: Exception =>
          log.warn(ex, "Encountered exception invoking the private ByteString constructor, falling back to safe method")
          slowByteArrayToByteString(byteArray)
      }
    else
      slowByteArrayToByteString(byteArray)
  }

  private final def slowByteArrayToByteString(byteArray: Array[Byte]): ByteString = {
    ByteString.copyFrom(byteArray)
  }

  private final def fastByteStringToByteArray(byteString: ByteString): Array[Byte] = {
    if (byteStringField != null)
      try {
        byteString.getClass() match {
          case ByteStringClass | LiteralByteStringClass =>
            // Protobuf 2.4 uses plain ByteString. 2.6 uses LiterealByteString (LiteralByteStringClass is null if 2.4 is used)
            byteStringField.get(byteString).asInstanceOf[Array[Byte]]
          case _ =>
            // 2.6 may use "RopeByteString" too.. Not sure if we would actually see that anytime, but warn and use regular method if so.
            log.warn("Encountered ByteString of class " + byteString.getClass().getName()+", falling back to safe method")
            slowByteStringToByteArray(byteString)
        }
      } catch {
        case ex: Exception =>
          log.warn(ex, "Encountered exception accessing the private ByteString bytes field, falling back to safe method")
          slowByteStringToByteArray(byteString)
      }
    else
      slowByteStringToByteArray(byteString)
  }

  private final def slowByteStringToByteArray(byteString: ByteString): Array[Byte] = {
    byteString.toByteArray
  }
}
