package br.com.freela.contrato.infrastructure.web;

import br.com.freela.contrato.application.ContratoNaoEncontradoException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
public class ApiExceptionHandler {
    private static final Logger log = LoggerFactory.getLogger(ApiExceptionHandler.class);

    @ExceptionHandler(ContratoNaoEncontradoException.class)
    public ProblemDetail naoEncontrado(ContratoNaoEncontradoException ex) {
        log.warn("http.erro.nao-encontrado motivo={}", ex.getMessage());
        return ProblemDetail.forStatusAndDetail(HttpStatus.NOT_FOUND, ex.getMessage());
    }

    @ExceptionHandler(IllegalStateException.class)
    public ProblemDetail transicaoInvalida(IllegalStateException ex) {
        log.warn("http.erro.transicao-invalida motivo={}", ex.getMessage());
        return ProblemDetail.forStatusAndDetail(HttpStatus.CONFLICT, ex.getMessage());
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ProblemDetail dadoInvalido(IllegalArgumentException ex) {
        log.warn("http.erro.dado-invalido motivo={}", ex.getMessage());
        return ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, ex.getMessage());
    }
}