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

public interface IEncoder
{
    /**
     * Returns a batch of binary data encapsulated as a {@link Transfer} object.
     * This object encapsulates the supplied buffer.
     *
     * If no buffer is supplied (buffer is null) encoder will provide buffer of its own.
     *
     * Function returns null when a new message is required.
     * @param buffer Place to write data
     * @return the passed-in buffer encapsulated in {@link Transfer} object; null when a new message is required
     */
    Transfer encode(ByteBuffer buffer);

    /**
     * Loads a new message into decoder
     */
    void loadMessage(Msg message);

    boolean hasData();
}
