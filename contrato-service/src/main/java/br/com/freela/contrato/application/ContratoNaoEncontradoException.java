package br.com.freela.contrato.application;

import java.util.UUID;

public class ContratoNaoEncontradoException extends RuntimeException {
    public ContratoNaoEncontradoException(UUID id) {
        super("Contrato não encontrado: " + id);
    }
}