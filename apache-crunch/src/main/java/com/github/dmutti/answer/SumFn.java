/* Copyright (c) - UOL Inc,
 * Todos os direitos reservados
 *
 * Este arquivo e uma propriedade confidencial do Universo Online Inc.
 * Nenhuma parte do mesmo pode ser copiada, reproduzida, impressa ou
 * transmitida por qualquer meio sem autorizacao expressa e por escrito
 * de um representante legal do Universo Online Inc.
 *
 * All rights reserved
 *
 * This file is a confidential property of Universo Online Inc.
 * No part of this file may be reproduced or copied in any form or by
 * any means without written permission from an authorized person from
 * Universo Online Inc.
 */
package com.github.dmutti.answer;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;


/**
 * @author dmutti@uolinc.com
 */
public class SumFn extends DoFn<String, Long> {

    private static final long serialVersionUID = 1L;

    @Override
    public void process(String arg, Emitter<Long> emitter) {
        emitter.emit(1L);
    }

}
