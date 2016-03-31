/*
    Copyright (c) 2007-2014 Contributors as noted in the AUTHORS file

    This file is part of 0MQ.

    0MQ is free software; you can redistribute it and/or modify it under
    the terms of the GNU Lesser General Public License as published by
    the Free Software Foundation; either version 3 of the License, or
    (at your option) any later version.

    0MQ is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Lesser General Public License for more details.

    You should have received a copy of the GNU Lesser General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

package zmq;

import java.nio.ByteBuffer;

public interface IDecoder
{
    public ByteBuffer getBuffer();

    /**
     * Decodes data pointed to by data_.
     * <p>
     * When a message is decoded, 1 is returned. When the decoder needs more data,
     * 0 is returned. On error, -1 is returned and errno is set accordingly.
     *
     * @param data_ Data to decode
     * @param size Number of bytes to decode
     * @param processed reference to the integer denoting the number of processed bytes
     * @return 1 on success, 0 when more data is needed, -1 on error (also sets errno)
     */
    int decode(ByteBuffer data_, int size, IntReference processed);

    Msg msg();
}
