import numpy as np
import time
import struct

BYTES_PER_PACKET = 1024
PACKET_HEADER_FORMAT = ">BBBBHHIIHHI"
HEADER_LENGTH = 24

class MeasurementPacket():
    def __init__(self, packet):
        (
            self.type,
            self.byte2,
            self.bram_i,
            self.num_brams,
            self.bram_offset,
            self.bram_depth,
            self.accum_num,
            self.master_counter,
            self.load_indicator,
            self.extra_param_18,
            self.extra_param_20,
        ) = struct.unpack(PACKET_HEADER_FORMAT, packet[:HEADER_LENGTH])
        

        self.data = packet[HEADER_LENGTH:]

    def __repr__(self):
        return self.__str__();

    def __str__(self):
        return  "(%s,%s)" % (str(chr(self.type)), str(self.accum_num))


class Measurement(object):
    def __init__(self, first_piece):
        self.timestamp = time.time()
        self.accum_num = first_piece.accum_num
        self.type = chr(first_piece.type)
        self.num_brams = first_piece.num_brams
        self.master_counter = first_piece.master_counter
        self.load_indicator = first_piece.load_indicator
        self.extra_param_18 = first_piece.extra_param_18
        self.extra_param_20 = first_piece.extra_param_20

        self.bram_length_bytes = max(first_piece.bram_depth * 4, 1024)
        self.packets_per_bram = max(self.bram_length_bytes / BYTES_PER_PACKET, 1)
        self.num_packets = self.packets_per_bram * self.num_brams

        # maps each packet to a boolean of whether it has arrived
        self.packet_map = np.array(
                [[False] * self.packets_per_bram] * self.num_brams)

        # stores the data
        self.brams = np.empty((self.num_brams, self.bram_length_bytes), 'uint8')
        # adds the first piece to the measurement
        self.place_piece(first_piece)

    def is_complete(self):
        return self.packet_map.all()

    def place_piece(self, piece):
        start = piece.bram_offset * 4
        end  = start +len(piece.data)#+ BYTES_PER_PACKET

        self.packet_map[piece.bram_i, start / 1024] = True
        a = np.fromstring(piece.data, dtype="uint8")
        
        self.brams[piece.bram_i,start:end] = a

    def __repr__(self):
        return self.__str__();

    def __str__(self):
        return  "(%s,%s)" % (str(self.type), str(self.accum_num))
