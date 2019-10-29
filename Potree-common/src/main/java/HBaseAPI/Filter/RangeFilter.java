/*
 * Copyright 2012 Shoji Nishimura
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package HBaseAPI.Filter;

import com.google.protobuf.InvalidProtocolBufferException;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.logging.FileHandler;
import java.util.logging.Logger;

/**
 * @author shoji
 * 
 */
public class RangeFilter extends FilterBase {


  public Logger logger = Logger.getLogger(RangeFilter.class.getName());

  {
    try {
      logger.addHandler(new FileHandler("/tmp/log1.txt"));

    }catch (Exception E){
      E.printStackTrace();
    }

  }

  public RangeFilter() {

  }

  private Range rx;
  private Range ry;

  public RangeFilter(Range rx, Range ry) {
    this.rx = rx;
    this.ry = ry;
  }

  /**
   * 将过滤器序列化为字节
   * @return
   * @throws IOException
   */
  @Override
  public byte[] toByteArray() throws IOException {
    MyFilterProtos.RangeFilter.Builder  builder = MyFilterProtos.RangeFilter.newBuilder();
    if(rx!=null&&ry!=null){
      builder.setXmin(rx.min);
      builder.setXmax(rx.max);
      builder.setYmin(ry.min);
      builder.setYmax(ry.max);
    }
    return builder.build().toByteArray();
  }

  /**
   * 从字节中反序列化rangefilter
   * @param pbBytes
   * @return
   * @throws DeserializationException
   */
  public static Filter parseFrom(byte[] pbBytes) throws DeserializationException {

    MyFilterProtos.RangeFilter proto;
    try {
      proto = MyFilterProtos.RangeFilter.parseFrom(pbBytes);
    }catch (InvalidProtocolBufferException e){
      throw new DeserializationException(e);
    }
    return new RangeFilter(new Range(proto.getXmin(),proto.getXmax()),new Range(proto.getYmin(),proto.getYmax()));
  }


  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)

  @Override
  public void readFields(DataInput in) throws IOException {
    int xmin = in.readInt();
    int xmax = in.readInt();
    int ymin = in.readInt();
    int ymax = in.readInt();

    this.rx = new Range(xmin, xmax);
    this.ry = new Range(ymin, ymax);
  }
*/
  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(rx.min);
    out.writeInt(rx.max);
    out.writeInt(ry.min);
    out.writeInt(ry.max);
  }
  */
  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.hadoop.hbase.filter.FilterBase#filterKeyValue(org.apache.hadoop
   * .hbase.KeyValue)
   */
  @Override
  public ReturnCode filterKeyValue(Cell cell) throws IOException {
    byte[] value = CellUtil.cloneValue(cell);

    int x = Bytes.toInt(value, 0);
    int y = Bytes.toInt(value, 4);
    if (rx.include(x) && ry.include(y)) {
      //logger.info(""+x+" "+y+" "+rx+" "+ry+" ReturnCode.INCLUDE");
      return ReturnCode.INCLUDE;
    } else {
      //logger.info(""+x+" "+y+" "+rx+" "+ry+" NEXT_ROW");
      return ReturnCode.NEXT_ROW;
    }

  }

}
