/**
 * Autogenerated by Thrift Compiler (0.12.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package jiffy.lease;

@SuppressWarnings({"cast", "rawtypes", "serial", "unchecked", "unused"})
@javax.annotation.Generated(value = "Autogenerated by Thrift Compiler (0.12.0)")
public class rpc_lease_ack implements org.apache.thrift.TBase<rpc_lease_ack, rpc_lease_ack._Fields>, java.io.Serializable, Cloneable, Comparable<rpc_lease_ack> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("rpc_lease_ack");

  private static final org.apache.thrift.protocol.TField RENEWED_FIELD_DESC = new org.apache.thrift.protocol.TField("renewed", org.apache.thrift.protocol.TType.I64, (short)1);
  private static final org.apache.thrift.protocol.TField LEASE_PERIOD_MS_FIELD_DESC = new org.apache.thrift.protocol.TField("lease_period_ms", org.apache.thrift.protocol.TType.I64, (short)2);

  private static final org.apache.thrift.scheme.SchemeFactory STANDARD_SCHEME_FACTORY = new rpc_lease_ackStandardSchemeFactory();
  private static final org.apache.thrift.scheme.SchemeFactory TUPLE_SCHEME_FACTORY = new rpc_lease_ackTupleSchemeFactory();

  public long renewed; // required
  public long lease_period_ms; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    RENEWED((short)1, "renewed"),
    LEASE_PERIOD_MS((short)2, "lease_period_ms");

    private static final java.util.Map<java.lang.String, _Fields> byName = new java.util.HashMap<java.lang.String, _Fields>();

    static {
      for (_Fields field : java.util.EnumSet.allOf(_Fields.class)) {
        byName.put(field.getFieldName(), field);
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, or null if its not found.
     */
    @org.apache.thrift.annotation.Nullable
    public static _Fields findByThriftId(int fieldId) {
      switch(fieldId) {
        case 1: // RENEWED
          return RENEWED;
        case 2: // LEASE_PERIOD_MS
          return LEASE_PERIOD_MS;
        default:
          return null;
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, throwing an exception
     * if it is not found.
     */
    public static _Fields findByThriftIdOrThrow(int fieldId) {
      _Fields fields = findByThriftId(fieldId);
      if (fields == null) throw new java.lang.IllegalArgumentException("Field " + fieldId + " doesn't exist!");
      return fields;
    }

    /**
     * Find the _Fields constant that matches name, or null if its not found.
     */
    @org.apache.thrift.annotation.Nullable
    public static _Fields findByName(java.lang.String name) {
      return byName.get(name);
    }

    private final short _thriftId;
    private final java.lang.String _fieldName;

    _Fields(short thriftId, java.lang.String fieldName) {
      _thriftId = thriftId;
      _fieldName = fieldName;
    }

    public short getThriftFieldId() {
      return _thriftId;
    }

    public java.lang.String getFieldName() {
      return _fieldName;
    }
  }

  // isset id assignments
  private static final int __RENEWED_ISSET_ID = 0;
  private static final int __LEASE_PERIOD_MS_ISSET_ID = 1;
  private byte __isset_bitfield = 0;
  public static final java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new java.util.EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.RENEWED, new org.apache.thrift.meta_data.FieldMetaData("renewed", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64)));
    tmpMap.put(_Fields.LEASE_PERIOD_MS, new org.apache.thrift.meta_data.FieldMetaData("lease_period_ms", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64)));
    metaDataMap = java.util.Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(rpc_lease_ack.class, metaDataMap);
  }

  public rpc_lease_ack() {
  }

  public rpc_lease_ack(
    long renewed,
    long lease_period_ms)
  {
    this();
    this.renewed = renewed;
    setRenewedIsSet(true);
    this.lease_period_ms = lease_period_ms;
    setLeasePeriodMsIsSet(true);
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public rpc_lease_ack(rpc_lease_ack other) {
    __isset_bitfield = other.__isset_bitfield;
    this.renewed = other.renewed;
    this.lease_period_ms = other.lease_period_ms;
  }

  public rpc_lease_ack deepCopy() {
    return new rpc_lease_ack(this);
  }

  @Override
  public void clear() {
    setRenewedIsSet(false);
    this.renewed = 0;
    setLeasePeriodMsIsSet(false);
    this.lease_period_ms = 0;
  }

  public long getRenewed() {
    return this.renewed;
  }

  public rpc_lease_ack setRenewed(long renewed) {
    this.renewed = renewed;
    setRenewedIsSet(true);
    return this;
  }

  public void unsetRenewed() {
    __isset_bitfield = org.apache.thrift.EncodingUtils.clearBit(__isset_bitfield, __RENEWED_ISSET_ID);
  }

  /** Returns true if field renewed is set (has been assigned a value) and false otherwise */
  public boolean isSetRenewed() {
    return org.apache.thrift.EncodingUtils.testBit(__isset_bitfield, __RENEWED_ISSET_ID);
  }

  public void setRenewedIsSet(boolean value) {
    __isset_bitfield = org.apache.thrift.EncodingUtils.setBit(__isset_bitfield, __RENEWED_ISSET_ID, value);
  }

  public long getLeasePeriodMs() {
    return this.lease_period_ms;
  }

  public rpc_lease_ack setLeasePeriodMs(long lease_period_ms) {
    this.lease_period_ms = lease_period_ms;
    setLeasePeriodMsIsSet(true);
    return this;
  }

  public void unsetLeasePeriodMs() {
    __isset_bitfield = org.apache.thrift.EncodingUtils.clearBit(__isset_bitfield, __LEASE_PERIOD_MS_ISSET_ID);
  }

  /** Returns true if field lease_period_ms is set (has been assigned a value) and false otherwise */
  public boolean isSetLeasePeriodMs() {
    return org.apache.thrift.EncodingUtils.testBit(__isset_bitfield, __LEASE_PERIOD_MS_ISSET_ID);
  }

  public void setLeasePeriodMsIsSet(boolean value) {
    __isset_bitfield = org.apache.thrift.EncodingUtils.setBit(__isset_bitfield, __LEASE_PERIOD_MS_ISSET_ID, value);
  }

  public void setFieldValue(_Fields field, @org.apache.thrift.annotation.Nullable java.lang.Object value) {
    switch (field) {
    case RENEWED:
      if (value == null) {
        unsetRenewed();
      } else {
        setRenewed((java.lang.Long)value);
      }
      break;

    case LEASE_PERIOD_MS:
      if (value == null) {
        unsetLeasePeriodMs();
      } else {
        setLeasePeriodMs((java.lang.Long)value);
      }
      break;

    }
  }

  @org.apache.thrift.annotation.Nullable
  public java.lang.Object getFieldValue(_Fields field) {
    switch (field) {
    case RENEWED:
      return getRenewed();

    case LEASE_PERIOD_MS:
      return getLeasePeriodMs();

    }
    throw new java.lang.IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new java.lang.IllegalArgumentException();
    }

    switch (field) {
    case RENEWED:
      return isSetRenewed();
    case LEASE_PERIOD_MS:
      return isSetLeasePeriodMs();
    }
    throw new java.lang.IllegalStateException();
  }

  @Override
  public boolean equals(java.lang.Object that) {
    if (that == null)
      return false;
    if (that instanceof rpc_lease_ack)
      return this.equals((rpc_lease_ack)that);
    return false;
  }

  public boolean equals(rpc_lease_ack that) {
    if (that == null)
      return false;
    if (this == that)
      return true;

    boolean this_present_renewed = true;
    boolean that_present_renewed = true;
    if (this_present_renewed || that_present_renewed) {
      if (!(this_present_renewed && that_present_renewed))
        return false;
      if (this.renewed != that.renewed)
        return false;
    }

    boolean this_present_lease_period_ms = true;
    boolean that_present_lease_period_ms = true;
    if (this_present_lease_period_ms || that_present_lease_period_ms) {
      if (!(this_present_lease_period_ms && that_present_lease_period_ms))
        return false;
      if (this.lease_period_ms != that.lease_period_ms)
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int hashCode = 1;

    hashCode = hashCode * 8191 + org.apache.thrift.TBaseHelper.hashCode(renewed);

    hashCode = hashCode * 8191 + org.apache.thrift.TBaseHelper.hashCode(lease_period_ms);

    return hashCode;
  }

  @Override
  public int compareTo(rpc_lease_ack other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = java.lang.Boolean.valueOf(isSetRenewed()).compareTo(other.isSetRenewed());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetRenewed()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.renewed, other.renewed);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = java.lang.Boolean.valueOf(isSetLeasePeriodMs()).compareTo(other.isSetLeasePeriodMs());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetLeasePeriodMs()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.lease_period_ms, other.lease_period_ms);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    return 0;
  }

  @org.apache.thrift.annotation.Nullable
  public _Fields fieldForId(int fieldId) {
    return _Fields.findByThriftId(fieldId);
  }

  public void read(org.apache.thrift.protocol.TProtocol iprot) throws org.apache.thrift.TException {
    scheme(iprot).read(iprot, this);
  }

  public void write(org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException {
    scheme(oprot).write(oprot, this);
  }

  @Override
  public java.lang.String toString() {
    java.lang.StringBuilder sb = new java.lang.StringBuilder("rpc_lease_ack(");
    boolean first = true;

    sb.append("renewed:");
    sb.append(this.renewed);
    first = false;
    if (!first) sb.append(", ");
    sb.append("lease_period_ms:");
    sb.append(this.lease_period_ms);
    first = false;
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    // alas, we cannot check 'renewed' because it's a primitive and you chose the non-beans generator.
    // alas, we cannot check 'lease_period_ms' because it's a primitive and you chose the non-beans generator.
    // check for sub-struct validity
  }

  private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException {
    try {
      write(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(out)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, java.lang.ClassNotFoundException {
    try {
      // it doesn't seem like you should have to do this, but java serialization is wacky, and doesn't call the default constructor.
      __isset_bitfield = 0;
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class rpc_lease_ackStandardSchemeFactory implements org.apache.thrift.scheme.SchemeFactory {
    public rpc_lease_ackStandardScheme getScheme() {
      return new rpc_lease_ackStandardScheme();
    }
  }

  private static class rpc_lease_ackStandardScheme extends org.apache.thrift.scheme.StandardScheme<rpc_lease_ack> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, rpc_lease_ack struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // RENEWED
            if (schemeField.type == org.apache.thrift.protocol.TType.I64) {
              struct.renewed = iprot.readI64();
              struct.setRenewedIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // LEASE_PERIOD_MS
            if (schemeField.type == org.apache.thrift.protocol.TType.I64) {
              struct.lease_period_ms = iprot.readI64();
              struct.setLeasePeriodMsIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          default:
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();

      // check for required fields of primitive type, which can't be checked in the validate method
      if (!struct.isSetRenewed()) {
        throw new org.apache.thrift.protocol.TProtocolException("Required field 'renewed' was not found in serialized data! Struct: " + toString());
      }
      if (!struct.isSetLeasePeriodMs()) {
        throw new org.apache.thrift.protocol.TProtocolException("Required field 'lease_period_ms' was not found in serialized data! Struct: " + toString());
      }
      struct.validate();
    }

    public void write(org.apache.thrift.protocol.TProtocol oprot, rpc_lease_ack struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      oprot.writeFieldBegin(RENEWED_FIELD_DESC);
      oprot.writeI64(struct.renewed);
      oprot.writeFieldEnd();
      oprot.writeFieldBegin(LEASE_PERIOD_MS_FIELD_DESC);
      oprot.writeI64(struct.lease_period_ms);
      oprot.writeFieldEnd();
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class rpc_lease_ackTupleSchemeFactory implements org.apache.thrift.scheme.SchemeFactory {
    public rpc_lease_ackTupleScheme getScheme() {
      return new rpc_lease_ackTupleScheme();
    }
  }

  private static class rpc_lease_ackTupleScheme extends org.apache.thrift.scheme.TupleScheme<rpc_lease_ack> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, rpc_lease_ack struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TTupleProtocol oprot = (org.apache.thrift.protocol.TTupleProtocol) prot;
      oprot.writeI64(struct.renewed);
      oprot.writeI64(struct.lease_period_ms);
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, rpc_lease_ack struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TTupleProtocol iprot = (org.apache.thrift.protocol.TTupleProtocol) prot;
      struct.renewed = iprot.readI64();
      struct.setRenewedIsSet(true);
      struct.lease_period_ms = iprot.readI64();
      struct.setLeasePeriodMsIsSet(true);
    }
  }

  private static <S extends org.apache.thrift.scheme.IScheme> S scheme(org.apache.thrift.protocol.TProtocol proto) {
    return (org.apache.thrift.scheme.StandardScheme.class.equals(proto.getScheme()) ? STANDARD_SCHEME_FACTORY : TUPLE_SCHEME_FACTORY).getScheme();
  }
}

